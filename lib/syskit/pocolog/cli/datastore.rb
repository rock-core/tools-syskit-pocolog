require 'roby'
require 'syskit'

require 'roby/cli/base'
require 'syskit/pocolog'
require 'syskit/pocolog/datastore/normalize'
require 'syskit/pocolog/datastore/import'
require 'tty-progressbar'
require 'pocolog/cli/null_reporter'
require 'pocolog/cli/tty_reporter'

module Syskit::Pocolog
    module CLI
        class Datastore < Roby::CLI::Base
            namespace 'datastore'

            desc 'normalize PATH [--out OUTPUT]', 'normalizes a data stream into a format that is suitable for the other log management commands to work'
            method_option :out, desc: 'output directory (defaults to a normalized/ folder under the source folder)',
                default: 'normalized'
            method_option :override, desc: 'whether existing files in the output directory should be overriden',
                type: :boolean, default: false
            method_option :silent, desc: 'do not display progress',
                type: :boolean, default: false

            def normalize(path)
                path = Pathname.new(path).realpath
                output_path = Pathname.new(options['out']).expand_path(path)
                output_path.mkpath

                paths = Syskit::Pocolog.logfiles_in_dir(path)
                if options[:silent]
                    reporter = Pocolog::CLI::NullReporter.new
                else
                    bytes_total = paths.inject(0) do |total, path|
                        total + path.size
                    end
                    reporter =
                        Pocolog::CLI::TTYReporter.new(
                            "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)", total: bytes_total)
                end
                begin
                    Syskit::Pocolog::Datastore.normalize(paths, output_path: output_path, reporter: reporter)
                ensure reporter.finish
                end
            end

            desc 'import DATASTORE_PATH PATH', 'normalize and import a raw dataset into a syskit-pocolog datastore'
            method_option :silent, desc: 'do not display progress',
                type: :boolean, default: false
            method_option :force, desc: 'overwrite existing datasets',
                type: :boolean, default: false
            def import(datastore_path, dataset_path)
                datastore_path = Pathname.new(datastore_path)
                datastore = Syskit::Pocolog::Datastore.create(datastore_path)
                dataset_path   = Pathname.new(dataset_path).realpath
                Syskit::Pocolog::Datastore.import(datastore, dataset_path, force: options[:force], silent: options[:silent])
            end

            desc 'auto-import DATASTORE_PATH PATH', 'import all folders looking like Syskit datasets under PATH into the datastore at DATASTORE_PATH'
            method_option :silent, desc: 'do not display progress',
                type: :boolean, default: false
            method_option :force, desc: 'overwrite existing datasets',
                type: :boolean, default: false
            method_option :min_duration, desc: 'ignore datasets that last for less than this many seconds. Defaults to one minutes, set to zero to disable',
                type: :numeric, default: 60
            def auto_import(datastore_path, root_path)
                root_path = Pathname.new(root_path).realpath
                datastore_path = Pathname.new(datastore_path)
                datastore_path.mkpath
                store = Syskit::Pocolog::Datastore.create(datastore_path)

                pastel = Pastel.new

                root_path.find do |p|
                    next if !p.directory?
                    next if !Pathname.enum_for(:glob, p + "*-events.log").any? { true }
                    next if !Pathname.enum_for(:glob, p + "*.0.log").any? { true }

                    if !options[:silent]
                        $stderr.puts pastel.bold("Processing #{p}")
                    end

                    last_import_digest, last_import_time = Syskit::Pocolog::Datastore::Import.find_import_info(p)
                    already_imported = (last_import_digest && store.has?(last_import_digest))
                    if already_imported && !options[:force]
                        if !options[:silent]
                            $stderr.puts pastel.yellow("#{p} already seem to have been imported as #{last_import_digest} at #{last_import_time}. Give --force to import again")
                        end
                        Find.prune
                    end

                    store.in_incoming do |core_path, cache_path|
                        importer = Syskit::Pocolog::Datastore::Import.new(store)
                        dataset = importer.normalize_dataset(p, core_path, cache_path: cache_path, silent: options[:silent])
                        stream_duration = dataset.each_pocolog_stream.map do |stream|
                            stream.duration_lg
                        end.max
                        stream_duration ||= 0

                        if already_imported
                            # --force is implied as otherwise we would have
                            # skipped earlier
                            $stderr.puts pastel.yellow("#{p} seem to have already been imported but --force is given, overwriting")
                            store.delete(last_import_digest)
                        end

                        if stream_duration >= options[:min_duration]
                            begin
                                importer.move_dataset_to_store(p, dataset, force: options[:force], silent: options[:silent])
                            rescue Syskit::Pocolog::Datastore::Import::DatasetAlreadyExists
                                $stderr.puts pastel.yellow("#{p} already seem to have been imported as #{dataset.compute_dataset_digest}. Give --force to import again")
                            end
                        elsif !options[:silent]
                            $stderr.puts pastel.yellow("#{p} lasts only %.1fs, ignored" % [stream_duration])
                        end
                    end

                    Find.prune
                end
            end
        end
    end
end

