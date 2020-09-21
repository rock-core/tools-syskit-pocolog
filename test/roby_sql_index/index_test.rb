# frozen_string_literal: true

require "test_helper"

module Syskit
    module Log
        module RobySQLIndex
            describe Index do
                before do
                    @index = Index.create(logfile_pathname("roby.sql"))
                end

                describe "model management" do
                    before do
                        @index.add_roby_log(roby_log_path("model_registration"))
                    end

                    it "registers a task instance model" do
                        assert_equal "Namespace::ChildModel",
                                     @index.models.by_name("Namespace::ChildModel")
                                           .one!.name
                    end
                end

                describe "event emission" do
                    before do
                        @index.add_roby_log(roby_log_path("event_emission"))
                    end

                    it "registers an emitted event" do
                        event = @index.emitted_events.by_name(:start).one!
                        assert_equal "start", event.name
                    end

                    it "builds its full name" do
                        event = @index.emitted_events.by_name(:start).one!
                        assert_equal "M.start_event", @index.event_full_name(event)
                    end

                    it "allows to get a task history" do
                        task = @index.tasks.one!
                        events = @index.history_of(task).to_a
                        assert(events.find { |ev| ev.name == "start" })
                        assert(events.find { |ev| ev.name == "stop" })
                    end
                end
            end
        end
    end
end
