require 'roby'
require 'syskit'

require 'roby/cli/base'
require 'syskit/pocolog'
require 'syskit/pocolog/datastore/normalize'
require 'syskit/pocolog/datastore/import'
require 'syskit/pocolog/datastore/index_build'
require 'tty-progressbar'
require 'pocolog/cli/null_reporter'
require 'pocolog/cli/tty_reporter'

module Syskit::Pocolog
    module CLI
        class Datastore < Roby::CLI::Base
            namespace 'datastore'

            no_commands do
                def create_reporter(format = "", silent: false, **options)
                    if silent
                        Pocolog::CLI::NullReporter.new
                    else
                        Pocolog::CLI::TTYReporter.new(format, **options)
                    end
                end

                def open_store(path)
                    path = Pathname.new(path).realpath
                    Syskit::Pocolog::Datastore.new(path)
                end

                def show_dataset(store, dataset, long_digest: false)
                    description = dataset.metadata_fetch_all('description', "<no description>")
                    if !long_digest
                        digest = store.short_digest(dataset)
                    end
                    format = "% #{digest.size}s %s"
                    description.zip([digest]) do |a, b|
                        puts format % [b, a]
                    end
                    metadata = dataset.metadata
                    metadata.each do |k, v|
                        next if k == 'description'
                        if v.size == 1
                            puts "  #{k}: #{v.first}"
                        else
                            puts "  #{k}:"
                            v.each do |value|
                                puts "  - #{value}"
                            end
                        end
                    end
                end

                def resolve_datasets(store, *query)
                    if query.empty?
                        return store.each_dataset
                    end

                    matchers = Hash.new
                    query.each do |kv|
                        if kv =~ /=/
                            k, v = kv.split('=')
                            matchers[k] = v
                        elsif kv =~ /~/
                            k, v = kv.split('~')
                            matchers[k] = /#{v}/
                        else # assume this is a digest
                            Syskit::Pocolog::Datastore::Dataset.
                                validate_encoded_short_digest(kv)
                            matchers['digest'] = /^#{kv}/
                        end
                    end
                    store.each_dataset.find_all do |dataset|
                        Hash['digest' => [dataset.digest]].merge(dataset.metadata).any? do |key, values|
                            if v_match = matchers[key]
                                values.any? { |v| v_match === v }
                            end
                        end
                    end
                end
            end

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
            method_option :auto, desc: 'import all datasets under PATH',
                type: :boolean, default: false
            method_option :silent, desc: 'do not display progress',
                type: :boolean, default: false
            method_option :force, desc: 'overwrite existing datasets',
                type: :boolean, default: false
            method_option :min_duration, desc: 'skip datasets whose duration is lower than this (in seconds)',
                type: :numeric, default: 60
            def import(datastore_path, root_path)
                root_path = Pathname.new(root_path).realpath
                if options[:auto]
                    paths = Array.new
                    root_path.find do |p|
                        is_raw_dataset = p.directory? &&
                            Pathname.enum_for(:glob, p + "*-events.log").any? { true } &&
                            Pathname.enum_for(:glob, p + "*.0.log").any? { true }
                        if is_raw_dataset
                            paths << p
                            Find.prune
                        end
                    end
                else
                    paths = [root_path]
                end

                pastel = Pastel.new

                datastore_path = Pathname.new(datastore_path)
                datastore = Syskit::Pocolog::Datastore.create(datastore_path)
                paths.each do |p|
                    if !options[:silent]
                        $stderr.puts pastel.bold("Processing #{p}")
                    end

                    last_import_digest, last_import_time = Syskit::Pocolog::Datastore::Import.find_import_info(p)
                    already_imported = (last_import_digest && datastore.has?(last_import_digest))
                    if already_imported && !options[:force]
                        if !options[:silent]
                            $stderr.puts pastel.yellow("#{p} already seem to have been imported as #{last_import_digest} at #{last_import_time}. Give --force to import again")
                            next
                        end
                    end

                    datastore.in_incoming do |core_path, cache_path|
                        importer = Syskit::Pocolog::Datastore::Import.new(datastore)
                        dataset = importer.normalize_dataset(p, core_path, cache_path: cache_path, silent: options[:silent])
                        stream_duration = dataset.each_pocolog_stream.map do |stream|
                            stream.duration_lg
                        end.max
                        stream_duration ||= 0

                        if already_imported
                            # --force is implied as otherwise we would have
                            # skipped earlier
                            $stderr.puts pastel.yellow("#{p} seem to have already been imported but --force is given, overwriting")
                            datastore.delete(last_import_digest)
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
                end
            end

            desc 'index DATASTORE_PATH [DATASETS]', 'refreshes or rebuilds (with --force) the datastore indexes'
            method_option :force, desc: 'force rebuilding even indexes that look up-to-date',
                type: :boolean, default: false
            method_option :silent, desc: 'suppress output',
                type: :boolean, default: false
            def index(datastore_path, *dataset_digests)
                datastore_path = Pathname.new(datastore_path).realpath
                store = Syskit::Pocolog::Datastore.new(datastore_path)
                datasets =
                    if dataset_digests.empty?
                        store.each_dataset.to_a
                    else
                        dataset_digests.map { |d| store.get(d) }
                    end
                datasets.each do |d|
                    Syskit::Pocolog::Datastore.index_build(store, d, force: options[:force])
                end
            end

            desc 'list DATASTORE_PATH [QUERY]', 'list datasets and their information'
            method_option :long_digest, desc: 'display digests in full form, instead of shortening them',
                type: :boolean, default: false
            def list(datastore_path, *query)
                store = open_store(datastore_path)
                datasets = resolve_datasets(store, *query)

                datasets.each do |dataset|
                    show_dataset(store, dataset, long_digest: options[:long_digest])
                end
            end

            desc 'metadata DATASTORE_PATH [QUERY] [--set=KEY=VALUE KEY=VALUE|--get=KEY]',
                'sets or gets metadata values for a dataset or datasets'
            method_option :set, desc: 'the key=value associations to set',
                type: :array
            method_option :get, desc: 'the keys to get',
                type: :array, lazy_default: []
            method_option :long_digest, desc: 'display digests in full form, instead of shortening them',
                type: :boolean, default: false
            def metadata(datastore_path, *query)
                if !options[:get] && !options[:set]
                    raise ArgumentError, "provide either --get or --set"
                elsif options[:get] && options[:set]
                    raise ArgumentError, "cannot provide both --get and --set at the same time"
                end

                store = open_store(datastore_path)
                datasets = resolve_datasets(store, *query)

                digest_to_s =
                    if options[:long_digest]
                        ->(d) { d.digest }
                    else
                        store.method(:short_digest)
                    end

                if options[:set]
                    setters = Hash.new
                    options[:set].map do |arg|
                        key, value = arg.split('=')
                        if !value
                            raise ArgumentError, "metadata setters need to be specified as key=value (got #{arg})"
                        end
                        (setters[key] ||= Set.new) << value
                    end

                    datasets.each do |set|
                        setters.each do |k, v|
                            set.metadata_set(k, *v)
                        end
                        set.metadata_write_to_file
                    end
                elsif options[:get].empty?
                    datasets.each do |set|
                        metadata = set.metadata.map { |k, v| [k, v.to_a.sort.join(",")] }.
                            sort_by(&:first).
                            map { |k, v| "#{k}=#{v}" }.
                            join(" ")
                        puts "#{digest_to_s[set]} #{metadata}"
                    end
                else
                    datasets.each do |set|
                        metadata = options[:get].map do |k, v|
                            [k, set.metadata_fetch_all(k, "<unset>")]
                        end
                        metadata = metadata.map { |k, v| "#{k}=#{v.to_a.sort.join(",")}" }.
                            join(" ")
                        puts "#{digest_to_s[set]} #{metadata}"
                    end
                end
            end
        end
    end
end

