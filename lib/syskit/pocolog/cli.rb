require 'roby'
require 'syskit'
require 'roby/cli/base'

require 'syskit/pocolog'
require 'tty-progressbar'
require 'log_tools/cli/tty_reporter'

module Syskit::Pocolog
    class CLI < Roby::CLI::Base
        no_commands do
            def setup_roby_for_running
                super
                app.using 'syskit'
                app.using 'syskit-pocolog'
            end
        end

        desc 'replay', 'replays a data replay script. If no script is given, allows to replay streams using profile definitions'
        def replay(*path)
            setup_roby_for_running

            paths = path.map { |p| Pathname.new(p) }
            if non_existent = paths.find { |p| !p.exist? }
                raise ArgumentError, "#{non_existent} does not exist"
            end

            script_paths, dataset_paths = paths.partition { |p| p.extname == '.rb' }

            app.setup
            begin
                streams = Syskit::Pocolog::Streams.new
                dataset_paths.each do |p|
                    if p.directory?
                        streams.add_dir(p)
                    else
                        streams.add_file(p)
                    end
                end

                if script_paths.empty?
                    # Load the default script
                    Syskit::Pocolog::Plugin.override_all_deployments_by_replay_streams(streams)
                else
                    script_paths.each do |p|
                        require p.to_s
                    end
                end
            rescue Exception
                app.cleanup
                raise
            end
            app.run
        end

        desc 'normalize', 'normalizes a data stream in a format that makes it easier and faster to use'
        method_option :out, desc: 'output directory (defaults to a normalized/ folder under the source folder)',
            default: 'normalized'
        method_option :override, desc: 'whether existing files in the output directory should be overriden',
            type: :boolean, default: false

        def normalize(path)
            path = Pathname.new(path).realpath
            output_path = Pathname.new(options['out']).expand_path(path)
            output_path.mkpath

            paths = Array.new
            Pathname.glob(path + '*.log') do |path|
                basename = path.basename
                if basename.to_s =~ /(.*)\.(\d+)\.log$/
                    paths << [$1, Integer($2), path]
                end
            end
            paths = paths.sort.map { |_, _, path| path }

            bytes_total = paths.inject(0) do |total, path|
                total + path.size
            end
            bytes_copied = 0

            last_progress_report = Time.now

            paths.each do |logfile_path|
                out_streams = Array.new
                reporter = LogTools::CLI::TTYReporter.new("|:bar| :current_byte/:total_byte :eta (:byte_rate/s)", total: logfile_path.size)
                logfile = Pocolog::Logfiles.new(logfile_path.open('r'))
                start = Time.now
                sample_count = 0
                logfile.each_data_block do |stream_index|
                    wio, _out_logfile = out_streams[stream_index]
                    if !wio
                        stream = logfile.streams[stream_index]
                        pocolog_out_file_path = output_path + Streams.normalized_filename(stream)
                        out_logfile = Pocolog::Logfiles.append(pocolog_out_file_path.to_s)
                        if out_logfile.streams.empty?
                            out_logfile.create_stream(stream.name, stream.type, stream.metadata)
                        end
                        wio = out_logfile.wio
                        out_streams[stream_index] = [wio, out_logfile]
                    end

                    payload = logfile.read_block_payload
                    Pocolog::Logfiles.write_block(
                        wio, Pocolog::STREAM_BLOCK, 0, payload)
                    sample_count += 1

                    now = Time.now
                    if (now - last_progress_report) > 0.1
                        header  = logfile.block_info
                        reporter.current = header.pos + bytes_copied
                        last_progress_report = now
                    end
                end
                out_streams.compact.each do |wio, out_logfile|
                    out_logfile.flush
                    out_logfile.write_index_file
                    out_logfile.close
                end
                out_streams.clear
                logfile.close
                puts "#{sample_count} samples (%.2f) samples/s" % [sample_count / (Time.now - start)]
            end
        end
    end
end

