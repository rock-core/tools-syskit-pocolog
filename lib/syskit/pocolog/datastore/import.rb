require 'syskit/pocolog/datastore/normalize'
require 'pocolog/cli/tty_reporter'

module Syskit::Pocolog
    class Datastore
        def self.import(datastore, dataset_path, silent: false, force: false)
            Import.new(datastore).import(dataset_path, silent: silent, force: force)
        end

        # Import dataset(s) in a datastore
        class Import
            class DatasetAlreadyExists < RuntimeError; end

            BASENAME_IMPORT_TAG = ".syskit-pocolog-import"

            attr_reader :datastore
            def initialize(datastore)
                @datastore = datastore
            end

            # Compute the information about what will need to be done during the
            # import
            def prepare_import(dir_path)
                pocolog_files = Syskit::Pocolog.logfiles_in_dir(dir_path)
                text_files    = Pathname.glob(dir_path + "*.txt")
                roby_files    = Pathname.glob(dir_path + "*-events.log")
                if roby_files.size > 1
                    raise ArgumentError, "more than one Roby event log found"
                end
                ignored = pocolog_files.map { |p| Pathname.new(::Pocolog::Logfiles.default_index_filename(p.to_s)) }
                ignored.concat roby_files.map { |p| p.sub(/-events.log$/, '-index.log') }

                all_files = Pathname.enum_for(:glob, dir_path + "*").to_a
                remaining = (all_files - pocolog_files - text_files - roby_files - ignored)
                return pocolog_files, text_files, roby_files.first, remaining
            end

            # Import a dataset into the store
            #
            # @param [Pathname] dir_path the input directory
            # @return [Pathname] the directory of the imported dataset in the store
            def import(dir_path, silent: false, force: false)
                datastore.in_incoming do |core_path, cache_path|
                    dataset = normalize_dataset(dir_path, core_path, cache_path: cache_path, silent: silent)
                    move_dataset_to_store(dir_path, dataset, force: force, silent: silent)
                end
            end

            # Find if a directory has already been imported
            #
            # @return [(String,Time),nil] if the directory has already been
            #   imported, the time and digest of the import. Otherwise, returns nil
            def self.find_import_info(path)
                info_path = (path + BASENAME_IMPORT_TAG)
                if info_path.exist?
                    info = YAML.load(info_path.read)
                    return info['sha2'], info['time']
                end
            end

            # Move the given dataset to the store
            #
            # @param [Pathname] dir_path the imported directory
            # @param [Dataset] dataset the normalized dataset, ready to be moved in
            #   the store
            # @param [Boolean] force if force (the default), the method will fail if
            #   the dataset is already in the store. Otherwise, it will erase the
            #   existing dataset with the new one
            # @return [Pathname] the path to the new dataset in the store
            # @raise DatasetAlreadyExists if a dataset already exists with the same
            #   ID than the new one and 'force' is false
            def move_dataset_to_store(dir_path, dataset, force: false, silent: false)
                dataset_digest = dataset.compute_dataset_digest

                if datastore.has?(dataset_digest)
                    if force
                        datastore.delete(dataset_digest)
                        if !silent
                            warn "Replacing existing dataset #{dataset_digest} with new one"
                        end
                    else
                        raise DatasetAlreadyExists, "a dataset identical to #{dataset.dataset_path} already exists in the store (computed digest is #{dataset_digest})"
                    end
                end

                final_core_dir = datastore.core_path_of(dataset_digest)
                FileUtils.mv dataset.dataset_path, final_core_dir
                final_cache_dir = datastore.cache_path_of(dataset_digest)
                if final_core_dir != final_cache_dir
                    FileUtils.mv dataset.cache_path, final_cache_dir
                end

                (dir_path + BASENAME_IMPORT_TAG).open('w') do |io|
                    YAML.dump(Hash['sha2' => dataset_digest, 'time' => Time.now], io)
                end

                final_core_dir
            end

            # Normalize the contents of the source folder into a dataset folder
            # structure
            #
            # It does not import the result into the store
            #
            # @param [Pathname] dir_path the input directory
            # @param [Pathname] output_dir_path the output directory
            # @return [Dataset] the resulting dataset
            def normalize_dataset(dir_path, output_dir_path, cache_path: output_dir_path, silent: false)
                pocolog_files, text_files, roby_event_log, ignored_entries =
                    prepare_import(dir_path)

                if !silent
                    puts "Normalizing pocolog log files"
                end
                pocolog_digests = normalize_pocolog_files(output_dir_path, pocolog_files, silent: silent, cache_path: cache_path)

                if roby_event_log
                    if !silent
                        puts "Copying the Roby event log"
                    end
                    roby_digests    = copy_roby_event_log(output_dir_path, roby_event_log)
                else roby_digests = Hash.new
                end

                if !silent
                    puts "Copying #{text_files.size} text files"
                end
                copy_text_files(output_dir_path, text_files)

                if !silent
                    puts "Copying #{ignored_entries.size} remaining files and folders"
                end
                copy_ignored_entries(output_dir_path, ignored_entries)

                dataset = Dataset.new(output_dir_path, cache: cache_path)
                digests = pocolog_digests.merge(roby_digests)
                metadata = digests.inject(Array.new) do |a, (path, digest)|
                    a << Dataset::IdentityEntry.new(path, path.size, digest.hexdigest)
                end

                dataset.weak_validate_identity_metadata(metadata)
                dataset.write_dataset_identity_to_metadata_file(metadata)

                roby_info_yml_path = (dir_path + "info.yml")
                if roby_info_yml_path.exist?
                    begin roby_info = YAML.load(roby_info_yml_path.read)
                    rescue Psych::SyntaxError
                        warn "failed to load Roby metadata from #{roby_info_yml_path}"
                    end
                    if roby_info && roby_info.respond_to?(:to_ary) && roby_info.first.respond_to?(:to_hash)
                        import_roby_metadata(dataset, roby_info.first.to_hash)
                    end
                end

                dataset.metadata_write_to_file
                dataset
            end

            # @api private
            #
            # Normalize pocolog files into the dataset
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input pocolog log files
            # @return [Hash<Pathname,Digest::SHA256>] a hash of the log file's
            #   pathname to the file's SHA256 digest. The pathnames are
            #   relative to output_dir
            def normalize_pocolog_files(output_dir, files, silent: false, cache_path: output_dir)
                return Hash.new if files.empty?

                out_pocolog_dir = (output_dir + "pocolog")
                out_pocolog_dir.mkpath
                out_pocolog_cache_dir = (cache_path + "pocolog")
                bytes_total = files.inject(0) { |s, p| s + p.size }
                reporter =
                    if silent
                        Pocolog::CLI::NullReporter.new
                    else
                        Pocolog::CLI::TTYReporter.new(
                            "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)", total: bytes_total)
                    end

                Syskit::Pocolog::Datastore.normalize(
                    files, output_path: out_pocolog_dir, index_dir: out_pocolog_cache_dir, reporter: reporter,
                    compute_sha256: true)
            ensure
                reporter.finish if reporter
            end

            # @api private
            #
            # Copy text files found in the input directory into the dataset
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input text file paths
            # @return [void]
            def copy_text_files(output_dir, files)
                if !files.empty?
                    out_text_dir    = (output_dir + "text")
                    out_text_dir.mkpath
                    FileUtils.cp files, out_text_dir
                end
            end

            # @api private
            #
            # Copy the Roby logs into the target directory
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input roby log files
            # @return [Hash<Pathname,Digest::SHA256>] a hash of the log file's
            #   pathname to the file's SHA256 digest
            def copy_roby_event_log(output_dir, event_log)
                target_file = output_dir + "roby-events.log"
                FileUtils.cp event_log, target_file
                digest = Digest::SHA256.new
                digest.update(event_log.read)
                Hash[target_file => digest]
            end

            # @api private
            #
            # Copy the entries in the input directory that are not recognized as a
            # dataset element
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input elements, which can be
            #   pointing to both files and directories. Directories are copied
            #   recursively
            # @return [void]
            def copy_ignored_entries(output_dir, paths)
                if !paths.empty?
                    out_ignored_dir = (output_dir + 'ignored')
                    out_ignored_dir.mkpath
                    FileUtils.cp_r paths, out_ignored_dir
                end
            end

            # Import the metadata from Roby into the dataset's own metadata
            def import_roby_metadata(dataset, roby_info)
                roby_info.each do |k, v|
                    dataset.metadata_add("roby:#{k}", v)
                end
            end
        end
    end
end

