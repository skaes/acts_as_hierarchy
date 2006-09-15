module RailsExpress
  module Acts #:nodoc:
    module Hierarchy #:nodoc:
      def self.included(base)
        super
        base.extend(ClassMethods)
      end

      class HierarchyException < Exception
      end

      module ClassMethods
        # Configuration options are:
        #
        # * +root_column+ - specifies the column name to use for identifying the root thread, default "root_id"
        # * +parent_column+ - specifies the column name to use for keeping the position integer, default "parent_id"
        # * +left_column+ - column name for left boundry data, default "lft"
        # * +right_column+ - column name for right boundry data, default "rgt"
        # * +depth+ - column name used to track the depth in the thread, default "depth"
        # * +scope+ - adds an additional contraint on the threads when searching or updating
        def acts_as_hierarchy(options = {})
          configuration = { :root_column => "root_id", :parent_column => "parent_id",
            :left_column => "lft", :right_column => "rgt", :depth_column => 'depth', :scope => "1 = 1"
          }
          configuration.update(options) if options.is_a?(Hash)
          configuration[:scope] = "#{configuration[:scope]}_id".intern if
            configuration[:scope].is_a?(Symbol) && configuration[:scope].to_s !~ /_id$/

          if configuration[:scope].is_a?(Symbol)
            scope_condition_method = %(
              def scope_condition
                if #{configuration[:scope].to_s}.nil?
                  "#{configuration[:scope].to_s} IS NULL"
                else
                  "#{configuration[:scope].to_s} = \#{#{configuration[:scope].to_s}}"
                end
              end
            )
          else
            scope_condition_method = "def scope_condition() \"#{configuration[:scope]}\" end"
          end

          class_eval <<-EOV
            include RailsExpress::Acts::Hierarchy::InstanceMethods

            #{scope_condition_method}

            def root_column() "#{configuration[:root_column]}" end
            def parent_column() "#{configuration[:parent_column]}" end
            def left_col_name() "#{configuration[:left_column]}" end
            def right_col_name() "#{configuration[:right_column]}" end
            def depth_column() "#{configuration[:depth_column]}" end

          EOV
        end
      end

      module InstanceMethods

        # check whether self is a root.
        def root?
          parent_id = self[parent_column]
          (parent_id == 0 || parent_id.nil?) && (self[left_col_name] == 1) && (self[right_col_name] > self[left_col_name])
        end

        # check whthether self is a child.
        def child?
          parent_id = self[parent_column]
          !(parent_id == 0 || parent_id.nil?) && (self[left_col_name] > 1) && (self[right_col_name] > self[left_col_name])
        end

        # check whether this record represents a one element set.
        def singleton?
          (self[left_col_name] == 1) && (self[right_col_name] == 2)
        end

        # check whether we represent a leaf.
        def leaf?
          self[left_col_name] == (self[right_col_name] - 1)
        end

        # check whether we have no idea what we represent.
        def unknown?
          !root? && !child?
        end

        # make it a singleton
        def before_create
          self[parent_column] ||= 0
          self[left_col_name] ||= 1
          self[right_col_name] ||= 2
          self[depth_column] ||= 0
        end

        # set root_id to self for newly created records
        def after_create
          raise HierarchyException, "Stinkin' internal error!" unless id
          parent_id = self[parent_column]
          if parent_id.zero?
            self[root_column] = id
            save
          else
            # Load the parent
            parent = self.class.find(parent_id)
            parent.add_child self
          end
        end

        # add a new child to self
        def add_child( child )
          reload unless new_record?
          child.reload unless child.new_record?

          raise HierarchyException, "Can't add to unknowns" if unknown? || new_record?
          raise HierarchyException, "Can't add unknowns" if child.unknown? || child.new_record?
          raise HierarchyException, "Can't add a non root" unless child.root?
          raise HierarchyException, "Can't add same item twice" if self[root_column] == child[root_column]

          self.class.transaction do
            # update elements to the right and up to the root
            diff = child.lft_rgt_mark_count
            self.class.update_all( "#{left_col_name} = (#{left_col_name} + #{diff})",  right_part_condition_lft )
            self.class.update_all( "#{right_col_name} = (#{right_col_name} + #{diff})", right_part_condition_rgt )
            # self[right_col_name] = self[right_col_name] + diff

            # increase depth of added subtree
            depth_increment = self[depth_column] + 1
            self.class.update_all( "#{depth_column} = #{depth_column} + #{depth_increment}" , child.subtree_condition)
            child[depth_column] = depth_increment

            # change lft and rgt values of subtree
            mark_increment = self[right_col_name] - 1
            # puts "%%% mark_increment: #{mark_increment} %%%"
            left_mark_action = "#{left_col_name} = (#{left_col_name} + #{mark_increment})"
            right_mark_action = "#{right_col_name} = (#{right_col_name} + #{mark_increment})"
            self.class.update_all( "#{left_mark_action} , #{right_mark_action}",  child.subtree_condition )

            child.reload
            # change the root of the added subtree
            new_root_id = self[root_column]
            self.class.update_all( "#{root_column} = #{new_root_id}" , child.subtree_condition)
            child.reload
            child[parent_column] = self.id
            child.save

          end
          self.reload
          child.reload
          self
        end

        # unlinks the subtree starting at self from its parent, keeping the unlinked subtree intact
        def unlink
          self.class.transaction do
            # new root for unlinked subtree
            new_root_id = id

            # change the root of the unlinked subtree
            self.class.update_all( "#{root_column} = #{new_root_id}" , subtree_condition)
            # attention: self still has the old root as an attribute in memory!

            # update elements to the right and up to the root
            diff = lft_rgt_mark_count
            self.class.update_all( "#{left_col_name} = (#{left_col_name} - #{diff})",  right_part_condition_lft )
            self.class.update_all( "#{right_col_name} = (#{right_col_name} - #{diff} )", right_part_condition_rgt )

            # now update child root id
            # from now on, conditions have new root embedded!
            reload
            raise HierarchyException, "Hell breaks loose" unless self[root_column]==new_root_id
            self[parent_column] = 0
            save

            # update depth of unlinked subtree
            depth_diff = self[depth_column]
            self.class.update_all( "#{depth_column} = #{depth_column} - #{depth_diff}" , subtree_condition)

            # decrease lft and rgt column values in unlinked tree
            lft_rgt_diff = self[left_col_name] - 1
            lft_action = "#{left_col_name} = #{left_col_name} - #{lft_rgt_diff}"
            rgt_action = "#{right_col_name} = #{right_col_name} - #{lft_rgt_diff}"
            self.class.update_all( "#{lft_action} , #{rgt_action}", subtree_condition)
          end
          reload
          self
        end

        # Returns the number of nested children of this object.
        def children_count
          size - 1
        end

        # number of node in subtree
        def size
          unknown? ? 1 : lft_rgt_mark_count/2
        end

        # Returns a set of itself and all of its nested children.
        # Children can optionally sorted by specifying +sort_proc+. Defaults to no sort.
        def full_set(sort_proc=nil)
          post_sort(self.class.find(:all, :conditions => subtree_condition, :order => left_col_name), sort_proc)
        end

        # Returns a set of all of its children and nested children
        # Children can optionally sorted by specifying +sort_proc+. Defaults to no sort.
        def all_children(sort_proc=nil)
          post_sort(self.class.find(:all, :conditions => all_kids_condition, :order => left_col_name), sort_proc)
        end

        # Returns a set of only this entry's immediate children
        # Children can optionally sorted by specifying +sort_proc+. Defaults to no sort.
        def direct_children(sort_proc=nil)
          post_sort(self.class.find(:all, :conditions => direct_kids_condition, :order => left_col_name), sort_proc)
        end

        # returns parent of self, if it exists.
        def parent
          parent_id = self[parent_column]
          parent_id and parent_id > 0 and self.class.find(parent_id)
        end

        # returns root of self
        def root
          root_id = self[root_column]
          self.class.find(root_id)
        end

        # create a tree wrapper from an array representing a nested set, such as returned by a full set operation.
        def envelope
          Envelope.build full_set
        end

        # Prunes a branch off of the tree, shifting all of the elements on the right
        # back to the left so the counts still work.
        def before_destroy
          return if self[right_col_name].nil? || self[left_col_name].nil?
          diff = lft_rgt_mark_count

          self.class.transaction do
            self.class.delete_all( all_kids_condition )
            self.class.update_all( "#{left_col_name} = (#{left_col_name} - #{diff})",  right_part_condition_lft )
            self.class.update_all( "#{right_col_name} = (#{right_col_name} - #{diff} )",  right_part_condition_rgt )
          end
        end

        # convert nested set to indented string representation.
        # useful for debugging.
        def to_ascii
          full_set.inject("") do |str, k|
            str << "#{"__" * k.depth}: id=#{k.id}, rt=#{k.root_id}, pr=#{k.parent_id}, lft=#{k.lft}, rgt=#{k.rgt}\n"
          end
        end

        protected

        # number of left and right marks in this set.
        def lft_rgt_mark_count
          self[right_col_name] - self[left_col_name] + 1
        end

        # SQL condition for restricting search to current tree
        def tree_condition
          "#{scope_condition} AND #{root_column} = #{self[root_column]}"
        end

        # SQL condition for retrieving subtree starting at self, including self
        def subtree_condition
          "#{tree_condition} AND (#{left_col_name} BETWEEN #{self[left_col_name]} and #{self[right_col_name]})"
        end

        # SQL condition for retrieving subtree starting at self, excluding self
        def all_kids_condition
          "#{tree_condition} AND #{left_col_name} > #{self[left_col_name]} AND #{right_col_name} < #{self[right_col_name]}"
        end

        # SQL condition for retrieving direct children
        def direct_kids_condition
          "#{scope_condition} AND #{parent_column} = #{self.id}"
        end

        # SQL condition for the nodes in the "right partition" whose lft mark is greater or equal to
        # self's rgt mark.
        def right_part_condition_lft
          "#{tree_condition} AND #{left_col_name} >= #{self[right_col_name]}"
        end

        # SQL condition for the nodes in the "right partition" whose rgt mark is greater or equal to
        # self's rgt mark.
        def right_part_condition_rgt
          "#{tree_condition} AND #{right_col_name} >= #{self[right_col_name]}"
        end

        # sorts +nested_set+ according to +sort_proc+.
        def post_sort(nested_set, sort_proc)
          if sort_proc
            Envelope.build(nested_set).sort_by(sort_proc).full_set
          else
            nested_set
          end
        end
      end

      # class Envelope wraps nested sets into tree structures.
      # it is used for post sorting sets retrieved from the database
      class Envelope
        attr_accessor :content, :kids, :parent

        # wrap a nested set in tree structure
        def self.build(nested_set)
          root = new(nested_set.shift)
          stack = [root]
          while content = nested_set.shift
            node = new(content)
            while (last = stack.last) && (last.content.id != node.content.parent_id)
              stack.pop
            end
            raise HierarchyException, "Internal Error" unless last
            last.kids << node
            node.parent = last
            stack << node
          end
          stack.first
        end

        # ctor
        def initialize(content, parent=nil)
          @content = content
          @parent = parent
          @kids = []
        end

        # create indented ascii representation of a tree.
        # useful for debugging.
        def to_ascii(indent=0)
          res = "#{'--' * indent}: #{content.id}"
          @kids.each{|k| res << "\n#{k.to_ascii(indent+1)}"}
          res
        end

        # sort children according to passed +proc+.
        def sort_by(proc)
          @kids = @kids.sort_by {|k| proc.call k.content}
          @kids.each {|k| k.sort_by proc}
          self
        end

        # create a nested set from this tree.
        def full_set(set = [])
          set << @content
          @kids.each{|k| k.full_set set}
          set
        end

        # find node for given +content_id+.
        def find(content_id)
          return self if content && (content.id==content_id)
          @kids.each{|k| if r = k.find(content_id) then return r end }
          return false
        end

        # same as find but raise error if not found.
        def safe_find(cid)
          if res = find(cid)
            return res
          else
            raise HierarchyException, "Did not find the stinkin' id"
          end
        end

        # unlink node from tree.
        def unlink
          raise HierarchyException, "Can't unlink a root" unless @parent
          raise HierarchyException, "Stinking Internal Error" unless @parent.kids.reject!{|k| k == self}
          @parent = nil
        end

        # add child to self. raises error if child already has another parent.
        def add_child(child)
          raise HierarchyException, "Stinking Internal Error" if child.parent
          child.parent = self
          @kids << child
        end

        # reverse order of chilren in the tree.
        def reverse
          @kids.reverse!
          @kids.each {|k| k.reverse}
          self
        end

        # reload all active record objects for givene tree.
        def reload
          @content.reload if content
          @kids.each{|k| k.reload }
        end

        # check whether associated active record objects are labelled correctly.
        # works only for tress which haven't been post sortded yet.
        # for testing only.
        def proper_marks?
          reload
          if content.root?
            start_with = 1
          else
            start_with = content.lft
          end
          check_marks(start_with, content.root_id)
        end

        # helper for +proper_marks?+.
        # for testing only.
        def check_marks(n, rid)
          return false if content.root_id != rid
          return false if content.lft != n
          n += 1
          0.upto(@kids.length-1) do |i|
            return false unless n = @kids[i].check_marks(n, rid)
          end
          return false if content.rgt != n
          n += 1
        end

        # check whether is structurally euqivalent to +other+ tree.
        # for testing only
        def structure?(other)
          @kids.size == other.kids.size and ( @kids.zip(other.kids).all? {|a,b| a.structure? b} )
        end

        # create a tree from array structure.
        #
        #    [[],[]] -->   o
        #                 / \
        #                o   o
        #
        # for testing only.
        def self.build_structure(s)
          tree = new nil
          tree.kids = s.map{|k| r = self.build_structure k; r.parent = tree; r }
          tree
        end
      end
    end
  end
end
