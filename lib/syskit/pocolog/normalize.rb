module Syskit::Pocolog
    # Encapsulation of the operations necessary to normalize a dataset
    class Normalize
        include Logger::Hierarchy
        extend Logger::Hierarchy
        class InvalidFollowupStream < RuntimeError; end

        attr_reader :out_files
        attr_reader :last_data_block_time
        attr_reader :stream_block_pos
        attr_reader :index_maps

        def initialize
            @out_files = Hash.new
            @last_data_block_time = Hash.new
            @stream_block_pos = Hash.new
            @index_maps = Hash.new
        end

        def normalize(paths, output_path: paths.first.dirname + "normalized", reporter: Pocolog::CLI::NullReporter.new)
            output_path.mkpath
            paths.each do |logfile_path|
                e, out_io = normalize_logfile(logfile_path, output_path, reporter: reporter)
                if e
                    warn "normalize: exception caught while processing #{logfile_path}, deleting #{out_io.size} output files: #{out_io.map(&:path).sort.join(", ")}"
                    out_io.each do |wio|
                        stream_block_pos.delete(wio)
                        index_maps.delete(wio)
                        out_files.delete_if { |p, io| io == wio }
                        wio.close
                        Pathname.new(wio.path).unlink
                        index_path = Pathname.new(wio.path.gsub(/\.log$/, '.idx'))
                        if index_path.exist?
                            index_path.unlink
                        end
                    end
                    raise e
                end
            end

            # Now write the indexes
            out_files.each do |out_path, (wio, _)|
                block_stream = Pocolog::BlockStream.new(wio)
                block_pos = stream_block_pos[wio]
                index_map = index_maps[wio]
                raw_stream_info = Pocolog::IndexBuilderStreamInfo.new(stream_block_pos[wio], index_maps[wio])
                stream_info = Pocolog.create_index_from_raw_info(block_stream, [raw_stream_info])
                File.open(Pocolog::Logfiles.default_index_filename(out_path.to_s), 'w') do |io|
                    Pocolog::Format::Current.write_index(io, block_stream.io, stream_info)
                end
            end

        ensure
            out_files.each_value do |io, stream_info|
                # Closed IO really means deleted file
                if !io.closed?
                    io.close
                end
            end
        end

        # @api private
        #
        # Normalize a single logfile
        #
        # It detects followup streams from previous calls. This is really
        # designed to be called by {#normalize}, and leaves a lot of cleanup to
        # {#normalize}. Do not call directly
        #
        # @return [(nil,Array<IO>),(Exception,Array<IO>)] returns a potential
        #   exception that has been raised during processing, and the IOs that
        #   have been touched by the call.
        def normalize_logfile(logfile_path, output_path, reporter: Pocolog::CLI::NullReporter.new)
            out_io_streams = Array.new

            reporter_offset = reporter.current
            last_progress_report = Time.now
            zero_index = [0].pack('v')

            in_block_stream = Pocolog::BlockStream.new(logfile_path.open)
            in_block_stream.read_prologue
            control_blocks = String.new
            followup_stream_time = Array.new
            in_streams = Array.new
            while block = in_block_stream.read_next_block_header
                stream_index = block.stream_index
                raw_payload  = in_block_stream.read_payload
                raw_block = block.raw_data + raw_payload

                # Control blocks must be saved in all generated log files
                # (they apply to all streams). Write them to all streams
                # seen so far, and write them when we (re)open an existing
                # file
                if block.kind == Pocolog::CONTROL_BLOCK
                    control_blocks << raw_block
                    out_io_streams.each do |wio|
                        wio.write raw_block
                    end
                    next
                end

                if block.kind == Pocolog::STREAM_BLOCK
                    wio = out_io_streams[stream_index] =
                        create_or_reuse_out_io(output_path, block.raw_data, raw_payload, control_blocks)

                    # If we're reusing a stream, save the time of the last
                    # written block so that we can validate that the two streams
                    # actually follow each other
                    followup_stream_time[stream_index] = last_data_block_time[wio]
                else
                    wio = out_io_streams[stream_index]
                    data_block_header = Pocolog::BlockStream::DataBlockHeader.parse(raw_payload)

                    # Second part of the followup stream validation (see above)
                    if last_stream_time = followup_stream_time[stream_index]
                        previous_rt, previous_lg = last_stream_time
                        if previous_rt > data_block_header.rt_time
                            raise InvalidFollowupStream, "found followup stream whose real time is before the stream that came before it"
                        elsif previous_lg > data_block_header.lg_time
                            raise InvalidFollowupStream, "found followup stream whose logical time is before the stream that came before it"
                        end
                    end

                    raw_block[2, 2] = zero_index
                    index_maps[wio] << wio.tell << data_block_header.lg_time
                    wio.write raw_block
                    last_data_block_time[wio] = [data_block_header.rt_time, data_block_header.lg_time]
                end

                now = Time.now
                if (now - last_progress_report) > 0.1
                    reporter.current = in_block_stream.tell + reporter_offset
                    last_progress_report = now
                end
            end
            return nil, out_io_streams
        rescue Exception => e
            return e, (out_io_streams || Array.new)
        ensure
            out_io_streams.each(&:flush)
            in_block_stream.close if in_block_stream
        end

        def create_or_reuse_out_io(output_path, raw_header, raw_payload, initial_blocks)
            stream_info = Pocolog::BlockStream::StreamBlock.parse(raw_payload)
            out_file_path = output_path + (Streams.normalized_filename(stream_info) + ".0.log")

            # Check if that's already known to us (multi-part
            # logfile)
            existing_wio, existing_stream = out_files[out_file_path]
            if existing_wio
                # This is a file we've already seen, reuse its info
                # and do some consistency checks
                if existing_stream.type != stream_info.type
                    raise InvalidFollowupStream, "multi-IO stream #{stream_info.name} is not consistent"
                end
                return existing_wio
            else
                wio = out_file_path.open('w+')
                begin
                    Pocolog::Format::Current.write_prologue(wio)
                    wio.write initial_blocks
                    stream_block_pos[wio] = wio.tell
                    index_maps[wio] = Array.new
                    raw_header[2, 2] = [0].pack("v")
                    wio.write raw_header
                    wio.write raw_payload
                    out_files[out_file_path] = [wio, stream_info]
                rescue Exception => e
                    wio.close
                    out_file_path.unlink
                    raise
                end
                wio
            end
        end
    end
end
