require 'syskit/pocolog/normalize'
require 'pocolog/cli/tty_reporter'

module Syskit::Pocolog
    def self.import(datastore_path, dataset_path)
        Import.new(datastore_path).import(dataset_path)
    end

    # Import dataset(s) in a datastore
    class Import
        class DatasetAlreadyExists < RuntimeError; end

        attr_reader :datastore_path
        def initialize(datastore_path)
            @datastore_path = datastore_path
        end

        # @api private
        #
        # Create a working directory in the incoming dir of the data store and
        # yield
        #
        # The created dir is deleted if it still exists after the block
        # returned. This ensures that no incoming leftovers are kept if an
        # opeartion fails
        def in_incoming(keep: false)
            incoming_dir = (datastore_path + "incoming")
            incoming_dir.mkpath

            i = 0
            begin
                while (import_dir = (incoming_dir + i.to_s)).exist?
                    i += 1
                end
                import_dir.mkdir
            rescue Errno::EEXIST
                i += 1
                retry
            end

            begin
                yield(import_dir)
            ensure
                if !keep && import_dir.exist?
                    import_dir.rmtree
                end
            end
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
            ignored = pocolog_files.map { |p| Pathname.new(Pocolog::Logfiles.default_index_filename(p.to_s)) }
            ignored.concat roby_files.map { |p| p.sub(/-events.log$/, '-index.log') }

            all_files = Pathname.enum_for(:glob, dir_path + "*").to_a
            remaining = (all_files - pocolog_files - text_files - roby_files - ignored)
            return pocolog_files, text_files, roby_files.first, remaining
        end

        # Import a dataset into the store
        def import(dir_path, silent: false)
            pocolog_files, text_files, roby_event_log, ignored_entries =
                prepare_import(dir_path)

            in_incoming do |output_dir_path|
                if !silent
                    puts "Normalizing pocolog log files"
                end
                pocolog_digests = normalize_pocolog_files(output_dir_path, pocolog_files, silent: silent)

                if roby_event_log
                    if !silent
                        puts "Copying the Roby event log"
                    end
                    roby_digests    = copy_roby_event_log(output_dir_path, roby_event_log)
                end

                if !silent
                    puts "Copying #{text_files.size} text files"
                end
                copy_text_files(output_dir_path, text_files)

                if !silent
                    puts "Copying #{ignored_entries.size} remaining files and folders"
                end
                copy_ignored_entries(output_dir_path, ignored_entries)

                dataset = Dataset.new(output_dir_path)
                digests = pocolog_digests.merge(roby_digests)
                metadata = digests.inject(Array.new) do |a, (path, digest)|
                    a << Dataset::IdentityEntry.new(path, path.size, digest.hexdigest)
                end

                dataset.weak_validate_identity_metadata(metadata)
                dataset.write_dataset_identity_to_metadata_file(metadata)
                dataset_digest = dataset.compute_dataset_digest
                if (datastore_path + dataset_digest).exist?
                    raise DatasetAlreadyExists, "a dataset identical to #{dir_path} already exists in the store (computed digest is #{dataset_digest})"
                end

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

                final_dir = (datastore_path + dataset_digest)
                FileUtils.mv output_dir_path, final_dir
                final_dir
            end
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
        def normalize_pocolog_files(output_dir, files, silent: false)
            return Hash.new if files.empty?

            out_pocolog_dir = (output_dir + "pocolog")
            out_pocolog_dir.mkpath
            bytes_total = files.inject(0) { |s, p| s + p.size }
            reporter =
                if silent
                    Pocolog::CLI::NullReporter.new
                else
                    Pocolog::CLI::TTYReporter.new(
                        "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)", total: bytes_total)
                end

            Syskit::Pocolog.normalize(
                files, output_path: out_pocolog_dir, reporter: reporter,
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

