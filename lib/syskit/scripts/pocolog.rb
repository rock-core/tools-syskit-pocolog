require 'roby'
require 'optparse'
require 'syskit'

app = Roby.app
app.require_app_dir
app.public_shell_interface = true
app.public_logs = true
app.using 'syskit'
app.using 'syskit-pocolog'

MetaRuby.keep_definition_location = false

options = OptionParser.new do |opt|
    opt.banner = <<-EOD
syskit pocolog [-r ROBOT] /path/to/dataset [script.rb]
    EOD
    Roby::Application.common_optparse_setup(opt)
end
remaining = options.parse(ARGV)

if remaining.empty?
    Syskit::Pocolog.fatal "dataset directory missing"
    puts options
    exit 1
elsif remaining.size > 1
    Syskit::Pocolog.fatal "too many arguments"
    puts options
    exit 1
end

paths = remaining.map { |p| Pathname.new(p) }
if non_existent = paths.find { |p| !p.exist? }
    Syskit::Pocolog.fatal "#{non_existent} does not exist"
    exit 1
end

script_paths, dataset_paths = paths.partition { |p| p.extname == '.rb' }
Roby.display_exception do
    app.setup
    begin
        streams = Syskit::Pocolog::Streams.new
        dataset_paths.each do |p|
            if p.directory?
                streams.add_dir(p)
            else
                streams.add_file(p)
            end
        end

        if script_paths.empty?
            # Load the default script
            Syskit::Pocolog::Plugin.override_all_deployments_by_replay_streams(streams)
        else
            script_paths.each do |p|
                require p.to_s
            end
        end
    rescue Exception
        app.cleanup
        raise
    end
    app.run
end


