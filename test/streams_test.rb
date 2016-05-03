require 'test_helper'

module Syskit::Pocolog
    describe Streams do
        subject { Streams.new }

        describe "#add_file" do
            it "adds the file's streams to the object" do
                logfile_path, logfile = create_log_file 'test'
                create_log_stream '/task.file', '/double'
                flush_log_file
                subject.add_file(logfile_path)
                assert_equal ['/task.file'], subject.each_stream.map(&:name)
            end

            it "raises ENOENT if the file does not exist" do
                assert_raises(Errno::ENOENT) { subject.add_file(Pathname('does_not_exist')) }
            end
        end

        describe ".from_dir" do
            it "creates a new streams object and adds the dir converted to pathname" do
                flexmock(Streams).new_instances.should_receive(:add_dir).once.with(Pathname.new('test'))
                assert_kind_of Streams, Streams.from_dir('test')
            end
        end

        describe ".from_file" do
            it "creates a new streams object and adds the file converted to pathname" do
                flexmock(Streams).new_instances.should_receive(:add_file).once.with(Pathname.new('test.0.log'))
                assert_kind_of Streams, Streams.from_file('test.0.log')
            end
        end

        describe "#add_dir" do
            it "raises ENOENT if the directory does not exist" do
                assert_raises(Errno::ENOENT) { subject.add_dir(Pathname.new("does_not_exist")) }
            end
            it "ignores the files that do not match the .NUM.log pattern" do
                create_log_dir
                FileUtils.touch((created_log_dir + "a.file").to_s)
                flexmock(subject).should_receive(:add_file_group).never
                subject.add_dir(created_log_dir)
            end
            it "adds files that match the .NUM.log pattern" do
                create_log_file 'test0'
                create_log_file 'test1'
                create_log_file 'test2'
                flexmock(subject).should_receive(:add_file_group).
                    with([created_log_dir + 'test0.0.log']).once
                flexmock(subject).should_receive(:add_file_group).
                    with([created_log_dir + 'test1.0.log']).once
                flexmock(subject).should_receive(:add_file_group).
                    with([created_log_dir + 'test2.0.log']).once
                subject.add_dir(created_log_dir)
            end

            it "opens files that belong together, together" do
                _, file = create_log_file 'test0'
                file.new_file
                create_log_file 'test1'
                flexmock(subject).should_receive(:add_file_group).
                    with([created_log_dir + 'test0.0.log', created_log_dir + "test0.1.log"]).once
                flexmock(subject).should_receive(:add_file_group).
                    with([created_log_dir + 'test1.0.log']).once
                subject.add_dir(created_log_dir)
            end
        end

        describe "#make_file_groups_in_dir" do
            it "groups files that have the same basename together" do
                _, file = create_log_file 'test0'
                file.new_file
                file.new_file
                create_log_file 'test1'
                groups = subject.make_file_groups_in_dir(created_log_dir)
                expected = [
                    [(created_log_dir + 'test0.0.log'), (created_log_dir + 'test0.1.log'), (created_log_dir + 'test0.2.log')],
                    [created_log_dir + 'test1.0.log']
                ]
                assert_equal expected, groups
            end
        end

        describe "#find_all_streams" do
            it "returns the streams that match the object" do
                logfile_path, _ = create_log_file 'test'
                create_log_stream '/task.file', '/double'
                create_log_stream '/other.task.file', '/double'
                create_log_stream '/does.not.match', '/double'
                flush_log_file
                subject.add_dir(created_log_dir)

                streams = subject.streams

                query = flexmock
                query.should_receive(:===).
                    with(->(s) { streams.include?(s) }).
                    and_return { |s| s != streams[2] }
                assert_equal streams[0, 2], subject.find_all_streams(query)
            end
        end

        describe "#find_task_by_name" do
            before do
                logfile_path, _ = create_log_file 'test'
                create_log_stream '/test0', '/double', 'rock_task_name' => "task"
                create_log_stream '/test1', '/double', 'rock_task_name' => "task"
                create_log_stream '/does.not.match', '/double', 'rock_task_name' => 'another_task'
                flush_log_file
                subject.add_dir(created_log_dir)
            end

            it "returns nil if there are no matching tasks" do
                assert !subject.find_task_by_name('does_not_exist')
            end

            it "returns a TaskStreams object with the matching streams" do
                streams = subject.find_task_by_name('task')
                assert_kind_of TaskStreams, streams
                assert_equal Set['/test0', '/test1'], streams.each_stream.map(&:name).to_set
            end

            describe "method_missing accessor" do
                it "returns the streams" do
                    streams = subject.task_task
                    assert_kind_of TaskStreams, streams
                    assert_equal Set['/test0', '/test1'], streams.each_stream.map(&:name).to_set
                end
                it "raises NoMethodError if no task exists" do
                    assert_raises(NoMethodError) do
                        subject.does_not_exist_task
                    end
                end
            end
        end
    end
end
