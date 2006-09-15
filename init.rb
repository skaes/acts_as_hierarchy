require 'hierarchy'

ActiveRecord::Base.class_eval do
  include RailsExpress::Acts::Hierarchy
end
