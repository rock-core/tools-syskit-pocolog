require 'test_helper'

module Syskit::Pocolog
    describe ReplayManager do
        attr_reader :subject, :streams, :port_stream, :task_m, :deployment_m
        before do
            double_t = Roby.app.default_loader.registry.get '/double'

            create_log_file 'test'
            create_log_stream '/port0', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'out',
                'rock_stream_type' => 'port'
            flush_log_file
            @streams = Streams.from_dir(created_log_dir).
                find_task_by_name('task')
            @port_stream = streams.find_port_by_name('out')
            @task_m = Syskit::TaskContext.new_submodel do
                output_port 'out', double_t
            end
            @subject = ReplayManager.new

            @deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_model: task_m, task_name: 'task')
            deployment_m.add_streams_from streams
        end

        describe "#register" do
            it "registers the stream-to-deployment mapping" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)
                assert_equal Hash[port_stream => Set[deployment_task, other_task]],
                    subject.stream_to_deployment
            end
            it "deregisters the stream if no deployment tasks are still using it" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                assert_equal Hash[port_stream => Set[deployment_task]], subject.stream_to_deployment
            end
            it "aligns the streams that are managed by the deployment task" do
                deployment_task = deployment_m.new
                flexmock(subject.stream_aligner).
                    should_receive(:add_streams).
                    with(streams.find_port_by_name('out')).
                    once
                subject.register(deployment_task)
            end
            it "does not realign the streams that are already in-use by another deployment" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)

                flexmock(subject.stream_aligner).
                    should_receive(:add_streams).
                    with().
                    once.pass_thru
                other_task = deployment_m.new
                subject.register(other_task)
            end
        end

        describe "#deregister" do
            it "removes the streams that are managed by the deployment task from the aligner" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                flexmock(subject.stream_aligner).
                    should_receive(:remove_streams).
                    with(streams.find_port_by_name('out')).
                    once
                subject.deregister(deployment_task)
            end
            it "does not deregister streams that are still in use by another deployment" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)

                flexmock(subject.stream_aligner).
                    should_receive(:remove_streams).
                    with().once.pass_thru
                subject.deregister(other_task)
            end
            it "deregisters the deployment task from the targets for the stream" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)
                subject.deregister(other_task)
                assert_equal Hash[port_stream => Set[deployment_task]], subject.stream_to_deployment
            end
            it "deregisters the stream if no deployment tasks are still using it" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                subject.deregister(deployment_task)
                assert_equal Hash[], subject.stream_to_deployment
            end
        end
    end
end

