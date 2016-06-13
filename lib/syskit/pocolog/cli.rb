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
        def normalize(path)
            path = Pathname.new(path).realpath
            output_path = Pathname.new(options['out']).expand_path(path)
            output_path.mkpath
            streams = Streams.from_dir(path)

            total_sample_count = streams.each_stream.inject(0) { |c, s| c + s.size }
            reporter = LogTools::CLI::TTYReporter.new("|:bar| :current/:total :eta", total: total_sample_count)

            last_progress_report = Time.now
            copied_stream_counter = 0
            streams.each_stream do |stream|
                task_name   = stream.metadata['rock_task_name'].gsub(/^\//, '')
                object_name = stream.metadata['rock_task_object_name']
                out_file_path = output_path + (task_name + "::" + object_name).gsub('/', ':')
                out_file = Pocolog::Logfiles.create(out_file_path.to_s)
                out_file_path = output_path + (task_name + "::" + object_name + ".0.log").gsub('/', ':')
                out_stream = out_file.create_stream(stream.name, stream.type, stream.metadata)

                reporter.log "copying #{stream.name} into #{out_file_path}"
                begin
                    stream.copy_to(out_stream) do |counter|
                        now = Time.now
                        if (now - last_progress_report) > 0.1
                            reporter.current = counter + copied_stream_counter
                            last_progress_report = now
                        end
                        false
                    end
                    copied_stream_counter += stream.size
                    out_file.flush
                    out_file.write_index_file
                    out_file.close
                    # Make generated files read-only
                    out_file_path.chmod 0o444
                    Pathname.new(out_file.default_index_filename).chmod 0o444
                rescue Interrupt
                    reporter.warn "interrupted, deleting #{out_file_path}"
                    out_file.close
                    out_file_path.unlink
                    raise
                rescue Exception
                    reporter.error "failed to copy #{out_file_path}, deleting"
                    out_file.close
                    out_file_path.unlink
                    raise
                end
            end
        ensure
            reporter.finish if reporter
        end
    end
end

