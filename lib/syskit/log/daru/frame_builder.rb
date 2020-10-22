# frozen_string_literal: true

module Syskit
    module Log
        module Daru
            # Utility class that allows to create a Daru dataframe based on
            # fields from a pocolog stream
            class FrameBuilder
                attr_reader :time_field
                attr_reader :vector_fields

                def initialize(type)
                    @type = type
                    @fields = []
                    @time_fields = []
                end

                # Tests whether there is already a column with a given name
                def column?(name)
                    @fields.any? { |f| f.name == name }
                end

                # This builder's column names
                def column_names
                    @fields.map(&:name)
                end

                # Save the stream's logical time in the given column
                def add_logical_time(name = "time")
                    add_resolved_field(LogicalTimeField.new(name))
                    @time_fields << (@fields.size - 1)
                end

                # Add a field that will be interpreted as time and shifted by center_time
                #
                # The field must represent microseconds in the same frame than
                # center_time
                def add_time_field(name = nil, &block)
                    field = add(name, &block)
                    @time_fields << (@fields.size - 1)
                end

                # Extract a field as a column in the resulting frame
                #
                # @param [String,nil] the column name. If it is not given, the column
                #    name is generated from the extracted fields (see below).
                # @yieldparam [PathBuilder] an object that allows to extract specific
                #    fields and/or apply transformations before the value gets
                #    stored in the frame
                def add(name = nil, &block)
                    raise ArgumentError, "a block is required" unless block_given?

                    resolved = resolve_field(&block)
                    resolved.name = name if name
                    add_resolved_field(resolved)
                end

                # @api private
                #
                # Register a resolved field
                #
                # @raise ArgumentError if the field's name is a duplicate
                def add_resolved_field(resolved)
                    if column?(resolved.name)
                        raise ArgumentError, "field #{name} already defined"
                    end

                    @fields << resolved
                    resolved
                end

                # @api private
                ResolvedField = Struct.new :name, :path, :type, :transform, :vector_transform do
                    def resolve(_time, value)
                        v = path.resolve(value).first.to_ruby
                        transform ? transform.call(v) : v
                    end

                    def create_vector(size)
                        if type <= Typelib::NumericType && !type.integer?
                            GSL::Vector.alloc(size)
                        else
                            Array.new(size)
                        end
                    end

                    def apply_vector_transform(vector)
                        vector_transform ? vector_transform.call(vector) : vector
                    end
                end

                class InvalidDataType < ArgumentError; end

                # @api private
                #
                # Helper that resolves a field from the block given to {#add} and {#time}
                #
                # @return [ResolvedField]
                def resolve_field
                    builder = yield(PathBuilder.new(@type))
                    unless builder.__terminal?
                        raise InvalidDataType,
                              "field resolved to type #{builder.__type}, "\
                              "which is not simple nor transformed"
                    end
                    ResolvedField.new(builder.__name, builder.__path,
                                      builder.__type,
                                      builder.__transform, builder.__vector_transform)
                end

                # @api private
                #
                # Called during conversion to a frame to create the target vectors
                # necessary to represent the data from this builder
                def create_vectors(size)
                    @fields.map { |f| f.create_vector(size) }
                end

                # @api private
                #
                # Called during resolution to update a data row
                def update_row(vectors, row, time, sample)
                    @fields.each_with_index do |f, i|
                        vectors[i][row] = f.resolve(time, sample)
                    end
                end

                # @api private
                #
                # Apply the center time on a time field if there is one
                def recenter_time_vectors(vectors, center_time)
                    center_time_usec =
                        center_time.tv_sec * 1_000_000 + center_time.tv_usec

                    @time_fields.each do |field_index|
                        converted = GSL::Vector.alloc(vectors[0].size)
                        vectors[field_index].each_with_index do |us, i|
                            converted[i] = Float(us - center_time_usec) / 1_000_000.0
                        end
                        vectors[field_index] = converted
                    end
                end

                # Convert the registered fields into a Daru frame
                #
                # @param [Time] center_time the time that should be used as
                #   zero in the frame index
                # @param [#raw_each] samples the object that will enumerate samples
                #   It must yield [realtime, logical_time, sample] the way
                #   Pocolog::SampleEnumerator does
                def to_daru_frame(center_time, streams)
                    Daru.create_aligned_frame(
                        center_time, [self], SingleStreamAdapter.new(streams.first),
                        streams.first.size
                    )
                end

                # @api private
                #
                # Adapter to resolve the logical time as a microseconds field
                class LogicalTimeField
                    attr_reader :name

                    def initialize(name)
                        @name = name
                    end

                    def resolve(time, _sample)
                        time.tv_sec * 1_000_000 + time.tv_usec
                    end

                    def create_vector(size)
                        Array.new(size)
                    end

                    def apply_vector_transform(vector)
                        vector
                    end
                end

                # @api private
                #
                # Adapter to provide a StreamAligner-like interface compatible
                # with daru's building procedure for a single pocolog stream
                class SingleStreamAdapter
                    def initialize(stream)
                        @stream = stream
                    end

                    def raw_each
                        @stream.raw_each do |_, lg, sample|
                            yield(0, lg, sample)
                        end
                    end
                end
            end
        end
    end
end
