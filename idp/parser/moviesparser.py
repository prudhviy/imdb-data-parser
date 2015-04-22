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

import re
import json
from .baseparser import *
from ..utils.regexhelper import RegExHelper


class MoviesParser(BaseParser):
    """
    Parses movies.list dump

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
        group 8: (.*)                                year
    """

    # properties
    base_matcher_pattern = "((.*? \(\S{4,}\)) ?(\(\S+\))? ?(?!\{\{SUSPENDED\}\})(\{(.*?) ?(\(\S+?\))?\})? ?(\{\{SUSPENDED\}\})?)\t+(.*)$"
    input_file_name = "movies.list"
    #FIXME: zafer: I think using a static number is critical for us. If imdb sends a new file with first 10 line fucked then we're also fucked
    number_of_lines_to_be_skipped = 15
    TYPE_TV_SERIES = '(TV_SERIES)'
    TYPE_TV_MOVIE = '(TV)' # TV movie (a single episode, produced for TV)
    TYPE_VIDEO = '(V)' # video movie (straight to video)
    TYPE_MOVIE = '(MOVIE)' # theatre movie

    db_table_info = {
        'tablename' : 'movies',
        'columns' : [
            {'colname' : 'title', 'colinfo' : DbScriptHelper.keywords['string'] + '(255) NOT NULL'},
            {'colname' : 'full_name', 'colinfo' : DbScriptHelper.keywords['string'] + '(127)'},
            {'colname' : 'type', 'colinfo' : DbScriptHelper.keywords['string'] + '(20)'},
            {'colname' : 'ep_name', 'colinfo' : DbScriptHelper.keywords['string'] + '(127)'},
            {'colname' : 'ep_num', 'colinfo' : DbScriptHelper.keywords['string'] + '(20)'},
            {'colname' : 'suspended', 'colinfo' : DbScriptHelper.keywords['string'] + '(20)'},
            {'colname' : 'year', 'colinfo' : DbScriptHelper.keywords['string'] + '(20)'}
        ],
        'constraints' : 'PRIMARY KEY(title)'
    }

    json_info = {
        'keys' : [
            {'title': 'string'},
            {'full_name': 'string'},
            {'type': 'string'},
            {'series_info': 'string'},
            {'ep_name': 'string'},
            {'ep_num': 'string'},
            {'suspended': 'string'},
            {'tv_series_years_active': 'string'}
        ]
    }
    end_of_dump_delimiter = "--------------------------------------------------------------------------------"

    def __init__(self, preferences_map):
        super(MoviesParser, self).__init__(preferences_map)
        self.first_one = True

    @staticmethod
    def split_movie_year(movie_year):
        """ Splits movie + year into regex groups and returns the matcher """
        year_pattern = '(.+)\s\((.+)\)$'
        year_matcher = RegExHelper(movie_year)
        is_match = year_matcher.match(year_pattern)

        if is_match:
            return year_matcher

        return None

    @staticmethod
    def get_year_released(movie_year):
        """ movie_year: has info as following, movie + (year) """
        matcher = MoviesParser.split_movie_year(movie_year)

        if matcher:
            return matcher.group(2)

        error = "something went wrong with year in movie parsing"
        print(error, movie_year)
        return error

    @staticmethod
    def get_movie_name(movie_year):
        """ Movie full_name has unecessary characters and year embedded in it
            This method returns clean movie name
        """
        matcher = MoviesParser.split_movie_year(movie_year)

        if matcher:
            non_movie_name_pattern = '^"(.+)"$'
            non_movie_name_matcher = RegExHelper(matcher.group(1))
            is_match = non_movie_name_matcher.match(non_movie_name_pattern)
            
            if is_match:
                return (non_movie_name_matcher.group(1)).lower()
            else:
                return (matcher.group(1)).lower()

        error = "something went wrong with movie name parsing"
        print(error, movie_year)
        return error

    @staticmethod
    def get_movie_type(matcher):
        """ Find out if the current line is about TV series or a Video Movie or Movie """
        movie_type_pattern = '(".+")' # check if the full_name is in quotes to determine if it is a TV series 
        movie_type_matcher = RegExHelper(matcher.group(2))
        is_match = movie_type_matcher.match(movie_type_pattern)

        movie_type = matcher.group(3)

        if is_match:
            if matcher.group(3) not in [MoviesParser.TYPE_TV_MOVIE, MoviesParser.TYPE_VIDEO]:
                movie_type = MoviesParser.TYPE_TV_SERIES
        else:
            if matcher.group(3) not in [MoviesParser.TYPE_TV_MOVIE, MoviesParser.TYPE_VIDEO]:
                movie_type = MoviesParser.TYPE_VIDEO

        return movie_type


    def parse_into_json(self, matcher):

        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            json_string = self.concat_regex_groups([1, 2, 3, 4, 5, 6, 7, 8], [1, 2, 3, 4, 5, 6, 7, 8], matcher, "movie")
            movie_info = json.loads(json_string)
            movie_info['type'] = MoviesParser.get_movie_type(matcher)
            
            movie_info['movie_name'] = MoviesParser.get_movie_name(matcher.group(2))
            
            if(movie_info['type'] != MoviesParser.TYPE_TV_SERIES):
                del movie_info['tv_series_years_active']

            movie_info['year_released'] = MoviesParser.get_year_released(matcher.group(2))
            self.json_file.write(json.dumps(movie_info) + '\n')
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1

    def parse_into_tsv(self, matcher):
        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            self.tsv_file.write(self.concat_regex_groups([1,2,3,5,6,7,8], None, matcher) + "\n")
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1

    def parse_into_db(self, matcher):
        is_match = matcher.match(self.base_matcher_pattern)

        if(is_match):
            if(self.first_one):
                self.sql_file.write("(" + self.concat_regex_groups([1,2,3,5,6,7,8], [0,1,2,3,4,5,6], matcher) + ")")
                self.first_one = False;
            else:
                self.sql_file.write(",\n(" + self.concat_regex_groups([1,2,3,5,6,7,8], [0,1,2,3,4,5,6], matcher) + ")")
        else:
            logging.critical("This line is fucked up: " + matcher.get_last_string())
            self.fucked_up_count += 1
