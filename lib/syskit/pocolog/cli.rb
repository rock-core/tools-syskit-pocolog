require 'roby'
require 'syskit'
require 'roby/cli/base'

require 'syskit/pocolog'
require 'syskit/pocolog/normalize'
require 'syskit/pocolog/import'
require 'tty-progressbar'
require 'pocolog/cli/null_reporter'
require 'pocolog/cli/tty_reporter'

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
            paths = path.map { |p| Pathname.new(p) }
            if non_existent = paths.find { |p| !p.exist? }
                raise ArgumentError, "#{non_existent} does not exist"
            end

            setup_roby_for_running
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

        desc 'normalize', 'normalizes a data stream into a format that is suitable for the other log management commands to work'
        method_option :out, desc: 'output directory (defaults to a normalized/ folder under the source folder)',
            default: 'normalized'
        method_option :override, desc: 'whether existing files in the output directory should be overriden',
            type: :boolean, default: false

        def normalize(path)
            path = Pathname.new(path).realpath
            output_path = Pathname.new(options['out']).expand_path(path)
            output_path.mkpath

            paths = Syskit::Pocolog.logfiles_in_dir(path)
            bytes_total = paths.inject(0) do |total, path|
                total + path.size
            end

            reporter = Pocolog::CLI::TTYReporter.new(
                "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)", total: bytes_total)
            begin
                Syskit::Pocolog.normalize(paths, output_path: output_path, reporter: reporter)
            ensure reporter.finish
            end
        end

        desc 'import', 'normalize and import a raw dataset into a syskit-pocolog datastore'
        def import(datastore_path, dataset_path)
            datastore_path = Pathname.new(datastore_path).realpath
            dataset_path   = Pathname.new(dataset_path).realpath

            Syskit::Pocolog.import(datastore_path, dataset_path)
        end
    end
end

