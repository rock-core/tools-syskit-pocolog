require 'test_helper'

module Syskit::Pocolog
    describe RockStreamMatcher do
        attr_reader :streams
        before do
            path, _ = create_log_file 'test'
            create_log_stream 'task.port', '/double', rock_stream_type: 'port', rock_task_object_name: 'port', rock_task_name: 'task'
            create_log_stream 'task.property', '/int', rock_stream_type: 'property', rock_task_object_name: 'property', rock_task_name: 'task'
            create_log_stream 'stream_without_properties', '/int'
            create_log_stream 'other_task.port', '/int', rock_task_name: 'other_task'
            create_log_stream 'stream_with_task_model', '/int', rock_task_model: 'orogen_model::Test'
            flush_log_file

            @streams = Streams.new
            streams.add_file path
        end
        subject { RockStreamMatcher.new }

        def assert_finds_streams(query, *stream_names)
            assert_equal stream_names, streams.find_all_streams(query).map(&:name)
        end

        describe "matching the stream type" do
            it "matches against ports" do
                assert_finds_streams subject.ports, 'task.port'
            end
            it "matches against properties" do
                assert_finds_streams subject.properties, 'task.property'
            end
            it "never matches streams that do not have the property" do
            end
            it "ORs ports and properties if both are specified" do
                assert_finds_streams subject.ports.properties, 'task.port', 'task.property'
            end
        end

        describe "matching the task name" do
            it "matches tasks that have the name" do
                assert_finds_streams subject.task_name('task'), 'task.port', 'task.property'
            end
            it "ORs the different names" do
                assert_finds_streams subject.task_name("task").task_name("other_task"), 'task.port', 'task.property', 'other_task.port'
            end
        end

        describe "matching the object name" do
            it "matches objects that have the name" do
                assert_finds_streams subject.object_name('port'), 'task.port'
            end
            it "ORs the different names" do
                assert_finds_streams subject.object_name("port").object_name("property"), 'task.port', 'task.property'
            end
        end

        describe "the task model" do
            it "matches the task model by name" do
                task_m = Syskit::TaskContext.new_submodel
                flexmock(task_m.orogen_model, :strict, name: 'orogen_model::Test')
                assert_finds_streams subject.task_model(task_m), 'stream_with_task_model'
            end
        end
    end
end

