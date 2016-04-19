require 'test_helper'

module Syskit::Pocolog
    describe Configuration do
        attr_reader :subject, :streams, :double_t
        before do
            @subject = Syskit::RobyApp::Configuration.new(Roby.app)
            double_t = Roby.app.default_loader.registry.get '/double'

            create_log_file 'test'
            create_log_stream '/port0', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object0',
                'rock_stream_type' => 'port'
            create_log_stream '/port1_1', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object1',
                'rock_stream_type' => 'port'
            create_log_stream '/port1_2', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object1',
                'rock_stream_type' => 'port'
            create_log_stream '/property0', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object0',
                'rock_stream_type' => 'property'
            create_log_stream '/property1_1', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object1',
                'rock_stream_type' => 'property'
            create_log_stream '/property1_2', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'object1',
                'rock_stream_type' => 'property'
            flush_log_file
            streams = Streams.from_dir(created_log_dir)
            @streams = streams.find_task_by_name('task')
        end

        describe "#use_pocolog_task" do
            it "declares the 'pocolog' process server" do
                task_m = Syskit::TaskContext.new_submodel
                subject.use_pocolog_task(streams, name: 'test', model: task_m)
                assert subject.has_process_server?('pocolog')
            end

            it "registers the stream-to-port mappings for the matching ports on the deployment model" do
                task_m = Syskit::TaskContext.new_submodel
                deployment_m = Deployment.new_submodel(task_model: task_m, task_name: 'test')
                flexmock(Syskit::Pocolog::Deployment).
                    should_receive(:new_submodel).
                    with(->(h) { h[:task_model] = task_m && h[:task_name] == 'test' }).
                    and_return(mock = flexmock(deployment_m))
                mock.should_receive(:add_streams_from).
                    with(streams, allow_missing: (flag = flexmock)).
                    once

                configured_deployment = subject.use_pocolog_task(streams, name: 'test', model: task_m, allow_missing: flag)
                assert_equal mock, configured_deployment.model
            end
        end
    end
end

