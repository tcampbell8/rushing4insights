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
        self.time_left_in_half = self.time_left_in_game % (30*60)
        if self.time_left_in_game == 60 * 60 or self.time_left_in_game == 60 * 30:
            self.time_left_in_half = 30 * 60


if __name__ == '__main__':
    years = range(2002, 2013)
    for year in years:
        with open(DATA_DIR+DATA[str(year)]) as f:
            lines = f.readlines()

        for count, line in enumerate(lines[1:]):
            p = Play()
            try:
                p.load_from_old_line(line)
            except PlayError:
                continue
            except Exception as e:
                print count, e
                print p.line
                continue
            print p.descripton
