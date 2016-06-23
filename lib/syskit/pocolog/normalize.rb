require 'digest/sha2'

module Syskit::Pocolog
    def self.normalize(paths, output_path: paths.first.dirname + "normalized", reporter: Pocolog::CLI::NullReporter.new, compute_sha256: false)
        Normalize.new.normalize(paths, output_path: output_path, reporter: reporter, compute_sha256: compute_sha256)
    end

    # Encapsulation of the operations necessary to normalize a dataset
    class Normalize
        include Logger::Hierarchy
        extend Logger::Hierarchy
        class InvalidFollowupStream < RuntimeError; end

        attr_reader :out_files

        Output = Struct.new :path, :wio, :stream_info, :digest, :stream_block_pos, :index_map, :last_data_block_time

        DigestIO = Struct.new :wio, :digest do
            def write(string)
                wio.write string
                digest.update string
            end
            def close
                wio.close
            end
            def flush
                wio.flush
            end
            def tell
                wio.tell
            end
            def closed?
                wio.closed?
            end
            def seek(pos)
                wio.seek(pos)
            end
            def read(count)
                wio.read(count)
            end
            def path
                wio.path
            end
            def stat
                wio.stat
            end
        end

        def initialize
            @out_files = Hash.new
        end

        def normalize(paths, output_path: paths.first.dirname + "normalized", reporter: Pocolog::CLI::NullReporter.new, compute_sha256: false)
            output_path.mkpath
            paths.each do |logfile_path|
                e, out_io = normalize_logfile(logfile_path, output_path, reporter: reporter, compute_sha256: compute_sha256)
                if e
                    warn "normalize: exception caught while processing #{logfile_path}, deleting #{out_io.size} output files: #{out_io.map(&:path).sort.join(", ")}"
                    out_io.each do |output|
                        out_files.delete(output.path)
                        wio = output.wio
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
            out_files.each_value do |output|
                wio = output.wio
                block_stream = Pocolog::BlockStream.new(wio)
                raw_stream_info = Pocolog::IndexBuilderStreamInfo.new(output.stream_block_pos, output.index_map)
                stream_info = Pocolog.create_index_from_raw_info(block_stream, [raw_stream_info])
                File.open(Pocolog::Logfiles.default_index_filename(output.path.to_s), 'w') do |io|
                    Pocolog::Format::Current.write_index(io, block_stream.io, stream_info)
                end
            end

            if compute_sha256
                result = Hash.new
                out_files.each_value.map { |output| result[output.path] = output.digest }
                result
            else
                out_files.each_value.map(&:path)
            end

        ensure
            out_files.each_value do |output|
                output.wio.close
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
        def normalize_logfile(logfile_path, output_path, reporter: Pocolog::CLI::NullReporter.new, compute_sha256: false)
            out_io_streams = Array.new

            reporter_offset = reporter.current
            last_progress_report = Time.now
            zero_index = [0].pack('v')

            in_io = logfile_path.open
            in_block_stream = Pocolog::BlockStream.new(in_io)
            begin
                in_block_stream.read_prologue
            rescue Pocolog::InvalidFile
                reporter.warn "#{logfile_path.basename} does not seem to be a valid pocolog file, skipping"
                reporter.current = in_io.size + reporter_offset
                return nil, Array.new
            end
            
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
                    out_io_streams[stream_index] = output =
                        create_or_reuse_out_io(output_path, block.raw_data, raw_payload, control_blocks, compute_sha256: compute_sha256)

                    # If we're reusing a stream, save the time of the last
                    # written block so that we can validate that the two streams
                    # actually follow each other
                    followup_stream_time[stream_index] = output.last_data_block_time
                else
                    output = out_io_streams[stream_index]
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
                    wio = output.wio
                    output.index_map << wio.tell << data_block_header.lg_time
                    wio.write raw_block
                    output.last_data_block_time = [data_block_header.rt_time, data_block_header.lg_time]
                end

                now = Time.now
                if (now - last_progress_report) > 0.1
                    reporter.current = in_block_stream.tell + reporter_offset
                    last_progress_report = now
                end
            end
            return nil, out_io_streams
        rescue Pocolog::NotEnoughData => e
            reporter.warn "#{logfile_path.basename} looks truncated (#{e.message}), stopping processing but keeping the samples processed so far"
            reporter.current = in_io.size + reporter_offset
            return nil, out_io_streams

        rescue Exception => e
            return e, (out_io_streams || Array.new)
        ensure
            out_io_streams.each { |output| output.wio.flush }
            in_block_stream.close if in_block_stream
        end

        def create_or_reuse_out_io(output_path, raw_header, raw_payload, initial_blocks, compute_sha256: false)
            stream_info = Pocolog::BlockStream::StreamBlock.parse(raw_payload)
            out_file_path = output_path + (Streams.normalized_filename(stream_info) + ".0.log")

            # Check if that's already known to us (multi-part
            # logfile)
            existing = out_files[out_file_path]
            if existing
                # This is a file we've already seen, reuse its info
                # and do some consistency checks
                if existing.stream_info.type != stream_info.type
                    raise InvalidFollowupStream, "multi-IO stream #{stream_info.name} is not consistent"
                end
                existing
            else
                wio = out_file_path.open('w+')

                begin
                    Pocolog::Format::Current.write_prologue(wio)
                    if compute_sha256
                        digest = Digest::SHA256.new
                        wio = DigestIO.new(wio, digest)
                    end
                    output = Output.new(out_file_path, wio, stream_info, digest, nil, Array.new, nil)

                    raw_header[2, 2] = [0].pack("v")
                    wio.write initial_blocks
                    output.stream_block_pos = wio.tell
                    wio.write raw_header
                    wio.write raw_payload
                    out_files[out_file_path] = output
                rescue Exception => e
                    wio.close
                    out_file_path.unlink
                    raise
                end
                output
            end
        end
    end
end
