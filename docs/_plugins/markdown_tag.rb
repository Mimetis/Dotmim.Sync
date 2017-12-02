module Jekyll
    class MarkdownTag < Liquid::Tag
      def initialize(tag_name, text, tokens)
        super
        @text = text.strip
      end
      require "kramdown"
      def render(context)
        tmpl = File.read File.join Dir.pwd, "_includes", @text
        Jekyll::Converters::Markdown::KramdownParser.new(Jekyll.configuration()).convert(tmpl)
      end
    end
  end
  Liquid::Template.register_tag('markdown', Jekyll::MarkdownTag)