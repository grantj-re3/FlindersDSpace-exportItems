#--
# Copyright (c) 2013, Flinders University, South Australia. All rights reserved.
# Contributors: eResearch@Flinders, Library, Information Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#++ 

##############################################################################
# Extend the File built-in class
class File
  # This method creates a new file, writes the specified string into it
  # then closes the file.
  # * Argument _filename_: The filename of the file to be created (or overwritten).
  # * Argument _str_: The string to be written into the file.
  def self.write_string(filename, str)
    begin
      File.open(filename, 'w').write(str)
    rescue Exception => ex
      STDERR.puts "Error writing to file: #{filename}\nPerhaps file is in use?\n#{ex}"
      exit 1
    end
  end
end  # class File

##############################################################################
# Extend the Object built-in class
class Object
  # Return a deep copy of this object (ie. all sub-objects are newly created)
  def deep_copy
    # Could use either Marshal or YAML (std lib) below
    Marshal.load( Marshal.dump(self) )
  end
end

##############################################################################
# Extend the String built-in class
class String
  @@xpath_prefix = ''

  # Set XPath prefix. To be used in conjunction with to_s_xpath().
  #
  # This is for a helper method for use with XPATH namespaces.
  def self.xpath_prefix=(prefix_str)
    @@xpath_prefix = prefix_str
  end

  # Convert:
  # * XPath string '/one/two/three' to '/pre:one/pre:two/pre:three'
  # * XPath string 'one/two/three'  to 'pre:one/pre:two/pre:three'
  # where argument or @@xpath_prefix = 'pre' or 'pre:'
  #
  # To be used in conjunction with xpath_prefix=().
  # This is for a helper method for use with XPATH namespaces.
  def to_s_xpath(xpath_prefix=@@xpath_prefix)
    # Append colon to the prefix unless prefix is empty or prefix already has one
    xpath_prefix += ':' unless xpath_prefix.empty? || xpath_prefix.match(/:$/)
    # Add prefix after every '/' & to start of xpath unless xpath starts with '/'
    self.gsub(/\//, "/#{xpath_prefix}").gsub(/^([^\/])/, "#{xpath_prefix}\\1")
  end

  # Force characters starting at the specified index and for the number of
  # characters given by length to lower case. By default, the first (leftmost)
  # character will be forced to lower case. Return self.
  # * Argument _index_: The starting character of the sequence to be converted
  #   to lower case. The first character of the string has an index of 0.
  # * Argument _length_: The number of characters to be converted to lower case.
  def downcase_chars!(index=0, length=1)
    self[index, length] = self[index, length].downcase
    self
  end

  # Force characters starting at the specified index and for the number of
  # characters given by length to lower case. By default, the first (leftmost)
  # character will be forced to lower case. Do not change the original
  # string.
  # * Argument _index_: The starting character of the sequence to be converted
  #   to lower case. The first character of the string has an index of 0.
  # * Argument _length_: The number of characters to be converted to lower case.
  def downcase_chars(index=0, length=1)
    s = String.new(self)
    s.downcase_chars!(index, length)
    s
  end

  # Force characters starting at the specified index and for the number of
  # characters given by length to upper case. By default, the first (leftmost)
  # character will be forced to upper case. Return self.
  # * Argument _index_: The starting character of the sequence to be converted
  #   to upper case. The first character of the string has an index of 0.
  # * Argument _length_: The number of characters to be converted to upper case.
  def upcase_chars!(index=0, length=1)
    self[index, length] = self[index, length].upcase
    self
  end

  # Force characters starting at the specified index and for the number of
  # characters given by length to upper case. By default, the first (leftmost)
  # character will be forced to upper case. Do not change the original
  # string.
  # * Argument _index_: The starting character of the sequence to be converted
  #   to upper case. The first character of the string has an index of 0.
  # * Argument _length_: The number of characters to be converted to upper case.
  def upcase_chars(index=0, length=1)
    s = String.new(self)
    s.upcase_chars!(index, length)
    s
  end

end  # class String

