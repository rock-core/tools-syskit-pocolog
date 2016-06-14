module Syskit::Pocolog
    # Encapsulation of the operations necessary to normalize a dataset
    class Normalize
        include Logger::Hierarchy

        attr_reader :out_files

        def initialize
            @out_files = Hash.new
        end

        def normalize(paths, output_path: path + "normalized", reporter: LogTools::NullReporter.new)
            bytes_copied = 0

            paths.each do |logfile_path|
                e, out_io = normalize_logfile(logfile_path, output_path, reporter: reporter)
                if e
                    warn "normalize: exception caught while processing #{logfile_path}, deleting #{out_io.size} output files: #{out_io.map(&:path).sort.join(", ")}"
                    out_io.each do |wio|
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

        ensure
            out_files.each_value do |io, stream_info, file_index|
                # Closed IO really means deleted file
                if !io.closed?
                    out_index_path = output_path + (Streams.normalized_filename(stream_info) + ".0.idx")
                    file_index.save(out_index_path)
                    io.close
                end
            end
        end

        def normalize_logfile(logfile_path, output_path, reporter: LogTools::CLI::NullReporter.new)
            reporter_offset = reporter.current
            last_progress_report = Time.now
            zero_index = [0].pack('v')

            in_block_stream = Pocolog::BlockStream.new(logfile_path.open('r'))
            in_block_stream.read_prologue
            control_blocks = String.new
            in_streams = Array.new
            out_io_streams = Array.new
            while block = in_block_stream.next
                stream_index = block.stream_index
                raw_payload  = in_block_stream.read_payload
                raw_block = block.raw_data + raw_payload

                # Control blocks must be saved in all generated log files
                # (they apply to all streams). Write them to all streams
                # seen so far, and write them when we (re)open an existing
                # file
                if block.kind == Pocolog::CONTROL_BLOCK
                    control_blocks << raw_block
                    out_io_streams.each do |wio, _stream_info, _file_index|
                        wio.write raw_block
                    end
                    next
                end

                if block.kind == Pocolog::STREAM_BLOCK
                    stream_info = Pocolog::BlockStream::StreamBlock.parse(raw_payload)
                    wio, _stream_info, file_index = out_io_streams[stream_index] =
                        create_or_reuse_out_io(output_path, stream_info, control_blocks)
                    raw_block[2, 2] = zero_index
                    pos = wio.tell
                    wio.write raw_block
                    file_index.add_stream(0, pos, 0)
                else
                    wio, _stream_definition, file_index =
                        out_io_streams[stream_index]
                    raw_block[2, 2] = zero_index
                    pos = wio.tell
                    wio.write raw_block
                    data_block = Pocolog::BlockStream::DataBlockHeader.parse(raw_payload)
                    file_index.add_block(0, pos, 0, data_block.rt_time, data_block.lg_time)
                end

                now = Time.now
                if (now - last_progress_report) > 0.1
                    reporter.current = in_block_stream.tell + reporter_offset
                    last_progress_report = now
                end
            end
            return nil, out_io_streams.map(&:first)
        rescue Exception => e
            return e, (out_io_streams || Array.new).map(&:first)
        ensure
            in_block_stream.close if in_block_stream
        end

        def create_or_reuse_out_io(output_path, stream_info, initial_blocks)
            out_file_path = output_path + (Streams.normalized_filename(stream_info) + ".0.log")

            # Check if that's already known to us (multi-part
            # logfile)
            if existing = out_files[out_file_path]
                # This is a file we've already seen, reuse its info
                # and do some consistency checks
                if !existing[1].valid_followup_stream?(stream_info)
                    raise "stream #{stream_info.name} found in #{logfile_path} is incompatible with already seen streams of the same type"
                end
                return existing
            else
                wio = out_file_path.open('w')
                file_index = Pocolog::FileIndexBuilder.new
                file_index.register_file(wio)
                Pocolog::BlockStream.write_prologue(wio)
                wio.write initial_blocks
                out_files[out_file_path] =
                    [wio, stream_info, file_index]
            end
        end
    end
end
