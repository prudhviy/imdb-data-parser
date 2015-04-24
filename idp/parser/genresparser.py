"""
This file is part of imdb-data-parser.

imdb-data-parser is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

imdb-data-parser is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with imdb-data-parser.  If not, see <http://www.gnu.org/licenses/>.
"""

from .baseparser import *
from .moviesparser import MoviesParser
import json


class GenresParser(BaseParser):
    """
    RegExp: /((.*? \(\S{4,}\)) ?(\(\S+\))? ?(?!\{\{SUSPENDED\}\})(\{(.*?) ?(\(\S+?\))?\})? ?(\{\{SUSPENDED\}\})?)\t+(.*)$/gm
    pattern: ((.*? \(\S{4,}\)) ?(\(\S+\))? ?(?!\{\{SUSPENDED\}\})(\{(.*?) ?(\(\S+?\))?\})? ?(\{\{SUSPENDED\}\})?)\t+(.*)$
    flags: gm
    8 capturing groups:
        group 1: #TITLE (UNIQUE KEY)
        group 2: (.*? \(\S{4,}\))                    movie name + year
        group 3: (\(\S+\))                           type ex:(TV)
        group 4: (\{(.*?) ?(\(\S+?\))?\})            series info ex: {Ally Abroad (#3.1)}
        group 5: (.*?)                               episode name ex: Ally Abroad
        group 6: ((\(\S+?\))                         episode number ex: (#3.1)
        group 7: (\{\{SUSPENDED\}\})                 is suspended?
        group 8: (.*)                                genre
    """

    # properties
    base_matcher_pattern = "((.*? \(\S{4,}\)) ?(\(\S+\))? ?(?!\{\{SUSPENDED\}\})(\{(.*?) ?(\(\S+?\))?\})? ?(\{\{SUSPENDED\}\})?)\t+(.*)$"
    input_file_name = "genres.list"
    number_of_lines_to_be_skipped = 378
    db_table_info = {
        'tablename' : 'genres',
        'columns' : [
            {'colname' : 'title', 'colinfo' : DbScriptHelper.keywords['string'] + '(255) NOT NULL'},
            {'colname' : 'genre', 'colinfo' : DbScriptHelper.keywords['string'] + '(127)'}
        ],
        'constraints' : 'PRIMARY KEY(title)'
    }

    json_info = {
        'keys' : [
            {'genre': 'string'}
        ]
    }

    end_of_dump_delimiter = ""

    def __init__(self, preferences_map):
        super(GenresParser, self).__init__(preferences_map)
        self.first_one = True

    def parse_into_json(self, matcher):

        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            #if(MoviesParser.get_movie_type(matcher.group(2), matcher.group(3)) == MoviesParser.TYPE_MOVIE):
            json_string = self.concat_regex_groups([8], [8], matcher, "genre")
            json_obj = json.loads(json_string)
            json_obj['year_released'] = MoviesParser.get_year_released(matcher.group(2))
            json_obj['movie_name'] = MoviesParser.get_movie_name(matcher.group(2))
            json_obj['movie_type'] = MoviesParser.get_movie_type(matcher.group(2), matcher.group(3))
            self.json_file.write(json.dumps(json_obj) + "\n")
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1

    def parse_into_tsv(self, matcher):
        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            self.tsv_file.write(self.concat_regex_groups([1,8], None, matcher) + "\n")
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1

    def parse_into_db(self, matcher):
        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            if(self.first_one):
                self.sql_file.write("(" + self.concat_regex_groups([1,8], [0,1], matcher) + ")")
                self.first_one = False;
            else:
                self.sql_file.write(",\n(" + self.concat_regex_groups([1,8], [0,1], matcher) + ")")
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1
