require 'test_helper'

module Syskit::Pocolog
    describe ShellInterface do
        attr_reader :app, :replay_manager, :subject

        before do
            @replay_manager = plan.execution_engine.pocolog_replay_manager
            @app = Roby::Application.new(plan: plan)
            @subject = ShellInterface.new(app)
        end

        it "gives access to the current replay" do
            flexmock(replay_manager).should_receive(:time).and_return(time = flexmock)
            assert_equal time, subject.time
        end
    end
end