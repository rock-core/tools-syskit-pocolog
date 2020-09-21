module Syskit
    module Log
        module RobySQLIndex
            # Access and creation API of the Roby SQL index
            class Index
                # Opens an existing index file, or creates one
                def self.open(path)
                    raise ArgumentError, "#{path} does not exist" unless path.exist?

                    rom = ROM.container(:sql, "sqlite://#{path}") do |config|
                        Definitions.configure(config)
                    end
                    new(rom)
                end

                # Create a new index file
                def self.create(path)
                    raise ArgumentError, "#{path} already exists" if path.exist?

                    rom = ROM.container(:sql, "sqlite://#{path}") do |config|
                        Definitions.schema(config)
                        Definitions.configure(config)
                    end
                    new(rom)
                end

                def initialize(rom)
                    @rom = rom
                    @models = rom.relations[:models]
                    @tasks = rom.relations[:tasks]
                    @emitted_events = rom.relations[:emitted_events]
                end

                def dispose
                    @db.close
                end

                # Access to models stored in the index
                #
                # @return [Models]
                attr_reader :models

                # Access to tasks stored in the index
                #
                # @return [Tasks]
                attr_reader :tasks

                # Access to emitted events stored in the index
                #
                # @return [EmittedEvents]
                attr_reader :emitted_events

                # Add information from a raw Roby log
                def add_roby_log(path, reporter: Pocolog::CLI::NullReporter.new)
                    require "roby/droby/logfile/reader"
                    require "roby/droby/plan_rebuilder"

                    @registered_models = {}
                    @registered_tasks = {}

                    size = path.stat.size
                    reporter.reset_progressbar("#{path.basename} [:bar]", total: size)

                    stream = Roby::DRoby::Logfile::Reader.open(path)
                    rebuilder = Roby::DRoby::PlanRebuilder.new

                    while (data = stream.load_one_cycle)
                        data.each_slice(4) do |m, sec, usec, args|
                            rebuilder.process_one_event(m, sec, usec, args)
                        end
                        rebuilder.plan.emitted_events.each do |ev|
                            add_log_emitted_event(ev)
                        end
                        rebuilder&.clear_integrated
                        reporter.current = stream.tell
                    end
                ensure
                    stream&.close
                end

                # @api private
                #
                # Add information about an emitted event
                #
                # @param [Roby::Event] ev
                # @return [Integer] the record ID
                def add_log_emitted_event(ev)
                    task_id = add_log_task(ev.generator.task)
                    @emitted_events.insert(
                        { name: ev.symbol.to_s, time: ev.time, task_id: task_id }
                    )
                end

                # @api private
                #
                # Add information about a task instance
                #
                # @param [Roby::Task] task
                # @return [Integer] the record ID
                def add_log_task(task)
                    if (task_id = @registered_tasks[task.droby_id])
                        return task_id
                    end

                    model_id = add_model(task.model)
                    @registered_tasks[task.droby_id] = @tasks.insert(
                        { model_id: model_id }
                    )
                end

                # @api private
                #
                # Add information about a Roby model
                #
                # @param [Class<Roby::Task>] model
                # @return [Integer] the record ID
                def add_model(model)
                    if (model_id = @registered_models[model.droby_id])
                        return model_id
                    end

                    match = @models.where(name: model.name).pluck(:id).first
                    return @registered_models[model.droby_id] = match if match

                    @registered_models[model.droby_id] =
                        @models.insert({ name: model.name })
                end

                # Return the events emitted by the given task
                def history_of(task)
                    if task.respond_to?(:pluck)
                        @emitted_events.where(task_id: task.pluck(:id))
                    else
                        @emitted_events.where(task_id: task.id)
                    end
                end

                # Tests whether there are events with the given name
                def event_with_name?(name)
                    @emitted_events.where(name: name).exist?
                end

                # Return the events emitted by the given task
                def tasks_by_model_name(name)
                    @tasks.where(model_id: @models.where(name: name).pluck(:id))
                end

                # Return the events emitted by the given task
                def tasks_by_model(model)
                    @tasks.where(model: model)
                end

                # Returns the full name of an event
                def event_full_name(event)
                    model_id = @tasks.by_pk(event.task_id).pluck(:id)
                    model_name = @models.by_pk(model_id).pluck(:name).first
                    "#{model_name}.#{event.name}_event"
                end
            end
        end
    end
end
