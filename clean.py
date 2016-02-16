import json

DATA_DIR = '/home/matt/workspace/analytics/data/nfl/'
DATA = {'2002': '2002_nfl_pbp_data.csv',
        '2003': '2003_nfl_pbp_data.csv',
        '2004': '2004_nfl_pbp_data.csv',
        '2005': '2005_nfl_pbp_data.csv',
        '2006': '2006_nfl_pbp_data.csv',
        '2007': '2007_nfl_pbp_data.csv',
        '2008': '2008_nfl_pbp_data.csv',
        '2009': '2009_nfl_pbp_data.csv',
        '2010': '2010_nfl_pbp_data.csv',
        '2011': '2011_nfl_pbp_data.csv',
        '2012': '2012_nfl_pbp_data.csv',
        '2013': 'pbp-2013.csv',
        '2014': 'pbp-2014.csv',
        '2015': 'pbp-2015.csv',
        '2013_old': '2013_nfl_pbp_data_through_wk_12.csv',
        }


class PlayError(Exception):
    pass


class NotLabelledError(Exception):
    pass


class Play(object):
    def load_from_old_line(self, line):
        cols = ['gameid', 'qtr', 'min', 'sec', 'off', 'def', 'down', 'togo',
                'ydline', 'description', 'offscore', 'defscore', 'season']
        vals = line.strip().split(',')
        try:
            self.down = int(vals[6])
        except ValueError:
            raise PlayError('No down means not a valid play.')
        self.line = vals
        self.description = vals[9]
        self.off_score = int(vals[10])
        self.def_score = int(vals[11])
        self.yard_line = int(vals[8]) if vals[8] is not '' else None
        self.dist_to_first = int(vals[7])
        self.score_diff = self.off_score - self.def_score
        self.quarter = int(vals[1])
        try:
            secs = int(vals[3])
        except ValueError:
            secs = 0
        self.time_left_in_game = 60 * int(vals[2]) + secs
        self.time_to_half = self.time_left_in_game % (30*60)
        if self.time_left_in_game == 60 * 60 or self.time_left_in_game == 60 * 30:
            self.time_to_half = 30 * 60

    def label(self):
        self.type = None
        rushing_types = ['left end', 'left tackle', 'left guard', 'right end', 'right tackle',
                         'right guard', 'up the middle', 'rushed']
        for t in rushing_types:
            if t in self.description:
                self.type = 'RUSH'
                self.is_pass = 0

        if 'pass' in self.description:
            self.type = 'PASS'
            self.is_pass = 1

        if self.type is None:
            raise NotLabelledError()

    def as_dict(self):
        ''' returns a dictionary of the attributes '''
        return {'time_to_half': self.time_to_half,
                'time_left_in_game': self.time_left_in_game,
                'down': self.down,
                'dist_to_first': self.dist_to_first,
                'quarter': self.quarter,
                'score_diff': self.score_diff,
                'yard_line': self.yard_line,
                'off_score': self.off_score,
                'def_score': self.def_score,
                'is_pass': self.is_pass}

    def as_csv(self):
        ''' returns a csv '''
        attrs = ['time_to_half', 'time_left_in_game', 'down',
                  'dist_to_first', 'quarter', 'score_diff',
                  'yard_line', 'off_score', 'def_score',
                 'is_pass']
        return ','.join([str(getattr(self, a)) for a in attrs])



if __name__ == '__main__':
    years = range(2002, 2013)
    total = 0
    not_label = 0
    plays = []
    for year in years:
        with open(DATA_DIR+DATA[str(year)]) as f:
            lines = f.readlines()

        for count, line in enumerate(lines[1:]):
            p = Play()
            try:
                p.load_from_old_line(line)
                p.label()
            except PlayError:
                continue
            except NotLabelledError:
                not_label += 1
                continue
            total += 1
            plays.append(p)
    print total
    print not_label

    with open('plays.json', 'w+') as f:
        '''f.write(','.join(['time_to_half', 'time_left_in_game', 'down',
                          'dist_to_first', 'quarter', 'score_diff',
                          'yard_line', 'off_score', 'def_score',
                         'is_pass']) + '\n')'''
        for p in plays:
            f.write(json.dumps(p.as_dict()) + '\n')
