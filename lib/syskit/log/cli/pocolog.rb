require 'roby'
require 'syskit'
require 'roby/cli/base'

require 'syskit/log'

module Syskit::Log
    module CLI
        class Pocolog < Roby::CLI::Base
            no_commands do
                def setup_roby_for_running
                    super
                    app.using 'syskit'
                    app.using 'syskit-pocolog'
                end
            end

            desc 'replay', 'replays a data replay script. If no script is given, allows to replay streams using profile definitions'
            def replay(*path)
                paths = path.map { |p| Pathname.new(p) }
                if non_existent = paths.find { |p| !p.exist? }
                    raise ArgumentError, "#{non_existent} does not exist"
                end

                setup_roby_for_running
                script_paths, dataset_paths = paths.partition { |p| p.extname == '.rb' }

                app.setup
                begin
                    streams = Syskit::Log::Streams.new
                    dataset_paths.each do |p|
                        if p.directory?
                            streams.add_dir(p)
                        else
                            streams.add_file(p)
                        end
                    end

                    if script_paths.empty?
                        # Load the default script
                        Syskit::Log::Plugin.override_all_deployments_by_replay_streams(streams)
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
        end
    end
end

