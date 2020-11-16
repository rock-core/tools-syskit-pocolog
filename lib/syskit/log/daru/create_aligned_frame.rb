# frozen_string_literal: true

module Syskit
    module Log
        module Daru
            def self.build_aligned_vectors(center_time, builders, joint_stream, size)
                current_row = Array.new(builders.size)
                initialized = false

                vectors = builders.map { |b| b.create_vectors(size) }

                row_count = 0
                joint_stream.raw_each do |index, time, sample|
                    current_row[index] = [time, sample]

                    force = (!initialized && !current_row.index(nil))
                    next unless force || (initialized && index == 0)

                    initialized = true

                    current_row.each_with_index do |(v_time, v_sample), v_index|
                        builders[v_index].update_row(
                            vectors[v_index], row_count, v_time, v_sample
                        )
                    end
                    row_count += 1
                end

                # Resize the vectors
                vectors = builders.zip(vectors).map do |b, v|
                    v = b.truncate_vectors(v, row_count)
                    b.recenter_time_vectors(v, center_time)
                    v
                end

                vectors.flatten(1)
            end

            def self.create_aligned_frame(center_time, builders, joint_stream, size)
                vectors = build_aligned_vectors(center_time, builders, joint_stream, size)
                names = builders.flat_map(&:column_names)

                ::Daru::DataFrame.new(Hash[names.zip(vectors)])
            end
        end
    end
end
