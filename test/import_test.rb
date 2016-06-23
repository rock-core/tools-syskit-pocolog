require 'test_helper'
require 'syskit/pocolog/import'
require 'tmpdir'

module Syskit::Pocolog
    describe Import do
        attr_reader :root_path, :datastore_path, :import

        before do
            @root_path = Pathname.new(Dir.mktmpdir)
            @datastore_path = root_path + 'datastore'
            @import = Import.new(datastore_path)
        end
        after do
            root_path.rmtree
        end

        describe "#in_incoming" do
            it "creates an incoming directory in the datastore and yields it" do
                import.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '0'), path
                    assert path.directory?
                end
            end
            it "handles having another process create a path concurrently" do
                (datastore_path + "incoming").mkpath
                called = false
                flexmock(Pathname).new_instances.should_receive(:mkdir).
                    and_return do
                        if !called
                            called = true
                            raise Errno::EEXIST
                        end
                    end

                import.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '1'), path
                end
            end
            it "ignores existing paths" do
                (datastore_path + "incoming" + "0").mkpath
                import.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '1'), path
                end
            end
            it "deletes the created path if it still exists at the end of the block" do
                created_path = import.in_incoming do |path|
                    path
                end
                refute created_path.exist?
            end
            it "does nothing if the path does not exist anymore at the end of the block" do
                created_path = import.in_incoming do |path|
                    FileUtils.mv path, (root_path + "safe")
                end
                assert (root_path + "safe").exist?
            end
        end

        describe "#prepare_import" do
            it "lists the pocolog files that should be copied, in normalized order" do
                FileUtils.touch(file0_1 = logfile_pathname('file0.1.log'))
                FileUtils.touch(file0_0 = logfile_pathname('file0.0.log'))
                FileUtils.touch(file1_0 = logfile_pathname('file1.0.log'))
                assert_equal [[file0_0, file0_1, file1_0], [], nil, []], import.prepare_import(logfile_pathname)
            end
            it "lists the test files that should be copied" do
                FileUtils.touch(path = logfile_pathname('file0.txt'))
                assert_equal [[], [path], nil, []], import.prepare_import(logfile_pathname)
            end
            it "lists the Roby log files that should be copied" do
                FileUtils.touch(path = logfile_pathname('test-events.log'))
                assert_equal [[], [], path, []], import.prepare_import(logfile_pathname)
            end
            it "ignores pocolog's index files" do
                FileUtils.touch(path = logfile_pathname('file0.1.log'))
                FileUtils.touch(logfile_pathname('file0.1.idx'))
                assert_equal [[path], [], nil, []], import.prepare_import(logfile_pathname)
            end
            it "ignores Roby index files" do
                FileUtils.touch(path = logfile_pathname('test-events.log'))
                FileUtils.touch(logfile_pathname('test-index.log'))
                assert_equal [[], [], path, []], import.prepare_import(logfile_pathname)
            end
            it "lists unrecognized files" do
                FileUtils.touch(path = logfile_pathname('not_matching'))
                assert_equal [[], [], nil, [path]], import.prepare_import(logfile_pathname)
            end
            it "lists unrecognized directories" do
                (path = logfile_pathname('not_matching')).mkpath
                assert_equal [[], [], nil, [path]], import.prepare_import(logfile_pathname)
            end
        end

        describe "#import" do
            before do
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', 
                        metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                end
                FileUtils.touch logfile_pathname('test.txt')
                FileUtils.touch logfile_pathname('test-events.log')
                FileUtils.touch logfile_pathname('not_recognized_file')
                logfile_pathname('not_recognized_dir').mkpath
                FileUtils.touch logfile_pathname('not_recognized_dir', 'test')
            end

            it "moves the results under the dataset's ID" do
                flexmock(Dataset).new_instances.should_receive(:compute_dataset_digest).
                    and_return('ABCDEF')
                import_dir = import.import(logfile_pathname, silent: true)
                assert_equal(datastore_path + 'ABCDEF', import_dir)
            end
            it "raises if the target dataset ID already exists" do
                flexmock(Dataset).new_instances.should_receive(:compute_dataset_digest).
                    and_return('ABCDEF')
                (datastore_path + "ABCDEF").mkpath
                assert_raises(Import::DatasetAlreadyExists) do
                    import.import(logfile_pathname, silent: true)
                end
            end
            it "reports its progress" do
                # This is not really a unit test. It just exercises the code
                # path that reports progress, but checks nothing except the lack
                # of exceptions
                capture_io do
                    import.import(logfile_pathname)
                end
            end
            it "normalizes the pocolog logfiles" do
                flexmock(Syskit::Pocolog).should_receive(:normalize).
                    with([logfile_pathname('test.0.log')], hsh(output_path: (datastore_path + 'incoming' + '0' + 'pocolog'))).once.
                    pass_thru
                import_dir = import.import(logfile_pathname, silent: true)
                assert (import_dir + 'pocolog' + 'task0::port.0.log').exist?
            end
            it "copies the text files" do
                import_dir = import.import(logfile_pathname, silent: true)
                assert logfile_pathname('test.txt').exist?
                assert (import_dir + 'text' + 'test.txt').exist?
            end
            it "copies the roby log files into roby-events.log" do
                import_dir = import.import(logfile_pathname, silent: true)
                assert logfile_pathname('test-events.log').exist?
                assert (import_dir + 'roby-events.log').exist?
            end
            it "copies the unrecognized files" do
                import_dir = import.import(logfile_pathname, silent: true)

                assert logfile_pathname('not_recognized_file').exist?
                assert logfile_pathname('not_recognized_dir').exist?
                assert logfile_pathname('not_recognized_dir', 'test').exist?

                assert (import_dir + 'ignored' + 'not_recognized_file').exist?
                assert (import_dir + 'ignored' + 'not_recognized_dir').exist?
                assert (import_dir + 'ignored' + 'not_recognized_dir' + 'test').exist?
            end
        end
    end
end