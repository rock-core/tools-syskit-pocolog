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

        describe "#each_task" do
            before do
                logfile_path, _ = create_log_file 'test'
                create_log_stream '/test0', '/double', 'rock_task_model' => 'project::Task', 'rock_task_name' => "task"
                create_log_stream '/test1', '/double', 'rock_task_model' => 'project::Task', 'rock_task_name' => "task"
                create_log_stream '/other_project', '/double', 'rock_task_model' => 'other_project::Task', 'rock_task_name' => 'other_task'
                create_log_stream '/not_task_model', '/double', 'rock_task_name' => 'task_without_model'
                flush_log_file
                subject.add_dir(created_log_dir)
            end

            # Helper method to test whether the method issues some warning
            # messages
            def should_warn(matcher)
                flexmock(Syskit::Pocolog).should_receive(:warn).with(matcher).once
            end

            it "ignores streams without a task model" do
                task_m = Syskit::TaskContext.new_submodel orogen_model_name: 'project::Task'
                other_task_m = Syskit::TaskContext.new_submodel orogen_model_name: 'other_project::Task'
                assert_equal ['task', 'other_task'], subject.each_task.map(&:task_name)
            end

            it "does not attempt to load the model's project if the task model is known" do
                Syskit::TaskContext.new_submodel orogen_model_name: 'project::Task'
                Syskit::TaskContext.new_submodel orogen_model_name: 'other_project::Task'
                flexmock(app).should_receive(:using_task_library).never
                subject.each_task.to_a
            end

            it "ignores streams that have a malformed rock_task_model name" do
                streams = Streams.new([s = subject.streams[0]])
                s.metadata['rock_task_model'] = ''
                should_warn /ignored 1 stream.*empty.*test0/
                flexmock(app).should_receive(:using_task_library).never
                assert_equal [], streams.each_task.to_a
            end

            it "does not attempt to load the model's project if load_models is false" do
                flexmock(app).should_receive(:using_task_library).never
                should_warn /ignored 2 streams.*project::Task.*\/test0, \/test1/
                should_warn /ignored.*other_project::Task.*other_project/
                assert_equal [], subject.each_task(load_models: false).to_a
            end

            it "attempts to load the model's project if load_models is true" do
                flexmock(app).should_receive(:using_task_library).once.
                    with('project').
                    and_return { Syskit::TaskContext.new_submodel(orogen_model_name: 'project::Task') }
                flexmock(app).should_receive(:using_task_library).once.
                    with('other_project')
                should_warn /ignored 1 stream.*other_project::Task.*other_project/
                assert_equal ['task'], subject.each_task(load_models: true).map(&:task_name)
            end

            it "groups the streams per task name" do
                task_m = Syskit::TaskContext.new_submodel orogen_model_name: 'project::Task'
                other_task_m = Syskit::TaskContext.new_submodel orogen_model_name: 'other_project::Task'
                task, other_task = subject.each_task.to_a
                assert_equal ['/test0', '/test1'], task.streams.map(&:name)
                assert_equal ['/other_project'], other_task.streams.map(&:name)
            end
        end
    end
end
