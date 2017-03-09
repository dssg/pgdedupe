from __future__ import division

import uuid
import random
import csv
from copy import copy
from datetime import date, timedelta
from enum import Enum


import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

fake = Faker()
nicknames = pd.read_csv('nicknames.csv', skipinitialspace=True)

class CategoricalDistribution(Enum):
    def __new__(cls, prob):
        # Extend the AutoNumber example to allow dupe values
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value, prob
        return obj
    @classmethod
    def __probabilities__(cls):
        total = sum(m.value[1] for m in cls)
        return [m.value[1]/total for m in cls]
class Sex(CategoricalDistribution):
    male = 50
    female = 50
# Values from http://www.census.gov/prod/cen2010/briefs/c2010br-02.pdf
class Race(CategoricalDistribution):
    white     = 223553265
    black     =  38929319
    amindian  =   2932248
    asian     =  14674252
    pacisland =    540013
    other     =  19107368 + 9009073
class Ethnicity(CategoricalDistribution):
    hispanic    =  50477594
    nonhispanic = 258267944
    
    @classmethod
    def __probabilities_by_race__(cls, race):
        if race == Race.white:
            vals = [29184290, 201856108]
        elif race == Race.black:
            vals = [1897218, 40123525]
        elif race == Race.amindian:
            vals = [1190904, 4029675]
        elif race == Race.asian:
            vals = [598146, 16722710]
        elif race == Race.pacisland:
            vals = [210307, 1014888]
        elif race == Race.other:
            vals = [20714218, 1033866]
        else: error()
        tot = sum(vals)
        return [v/tot for v in vals]

class Person(object):
    def __init__ (self,
            fname,
            lname,
            ssn,
            sex,
            dob,
            race,
            ethnicity):
        self.id = uuid.uuid4()
        self.fname = fname
        self.lname = lname
        self.ssn = ssn
        self.sex = sex
        self.dob = dob
        self.race = race
        self.ethnicity = ethnicity
        self.name_change_count = 0
        
    def write_row(self, csvfile):
        csvfile.writerow([
                self.id,
                self.munged_fname(),
                self.munged_lname(),
                self.munged_ssn(),
                self.munged_sex(),
                self.munged_dob(),
                self.munged_race(),
                self.munged_ethnicity(),
            ])
    
    def munged_fname(self):
        fname = self.fname
        if fname in nicknames.name.values and random.random() < .2:
            fname = random.choice(list(nicknames[nicknames.name == fname].nickname))
        if fname[-1] == 'y' and random.random() < 0.1:
            fname = fname[:-1] + 'ie'
        elif fname[-2:] == 'ie' and random.random() < 0.1:
            fname = fname[:-2] + 'y'
        return typo(fname)
    
    def munged_lname(self):
        if self.sex == Sex.female and self.name_change_count < 2 and random.random() < 0.05:
            # Name change. TODO: this is really a rate per year, not per record
            self.lname = fake.last_name()
            self.name_change_count = self.name_change_count + 1
        lname = self.lname
        if ' ' in self.lname:
            if random.random() < .4:
                lname = lname.split(' ')[0]
            elif random.random() < .2:
                lname = lname.split(' ')[1]
            elif random.random() < .1:
                lname = lname.replace(' ', '-')
        return typo(lname)
    
    def munged_ssn(self):
        if random.random() < .15: return None
        if random.random() < .01:
            chars = list(self.ssn)
            for i, d in enumerate(chars):
                if d == '-': continue
                if random.random() < 1/9:
                    chars[i] = str((int(d) + random.choice([-1,1])) % 10)
            return ''.join(chars)
        return self.ssn
    
    def munged_sex(self):
        if random.random() < .05: return None
        if random.random() < .001: return 'F' if self.sex == Sex.male else 'M'
        return self.sex.name[0].upper()
    
    def munged_dob(self):
        if random.random() < .05: return None
        dob = self.dob
        r = random.random()
        if dob.day <= 12 and r < 0.01:
            return date(dob.year,dob.day,dob.month)
        if dob.month < 12 and r < 0.02:
            return date(dob.year,dob.month+1,min(dob.day, 28))
        if dob.month > 1 and r < 0.03:
            return date(dob.year,dob.month-1,min(dob.day, 28))
        if dob.day < 28 and r < 0.04:
            return date(dob.year,dob.month,dob.day+1)
        if dob.day > 1 and r < 0.05:
            return date(dob.year,dob.month,dob.day-1)
        if dob.day > 10 and r < 0.06:
            return date(dob.year,dob.month,dob.day-10)
        if dob.day < 19 and r < 0.07:
            return date(dob.year,dob.month,dob.day+10)
        if r < 0.09:
            return date(dob.year + random.choice((-1,1)), dob.month, min(dob.day, 28))
        if r < 0.15: # Birthday issues are pretty common
            return dob + timedelta(days=random.normalvariate(0,365/2))
        return dob
    
    def munged_race(self):
        if random.random() < .2: return None
        if random.random() < .1: return random.choice(list(Race)).name
        return self.race.name
    
    def munged_ethnicity(self):
        if random.random() < .3: return None
        if random.random() < .1: return random.choice(list(Ethnicity)).name
        return self.ethnicity.name
    
    def twin(self):
        p = copy(self)
        # A new, different person
        p.id = uuid.uuid4()
        # Random gender and new name
        ismale = fake.boolean()
        while p.fname == self.fname: # ensure we don't give the same name
            p.fname = fake.first_name_male() if ismale else fake.first_name_female()
        p.sex = Sex.male if ismale else Sex.female
        # SSN off by one
        s = str(int(p.ssn.replace('-',''))+1).zfill(9)
        p.ssn = '-'.join((s[:3], s[3:5], s[5:]))
        return p
    
    @staticmethod
    def _rand():
        ismale = fake.boolean()
        fname = fake.first_name_male() if ismale else fake.first_name_female()
        race = np.random.choice(Race, 1, p=Race.__probabilities__())[0]
        ethn = np.random.choice(Ethnicity, 1, p=Ethnicity.__probabilities_by_race__(race))[0]
        # Let's just guess that 50% of hispanics have two last names, and a small number otherwise:
        lname = fake.last_name()
        if (ethn == Ethnicity.hispanic and random.random() < .5) or random.random() < .02:
            lname = lname + " " + fake.last_name()
        return Person(fname, lname, fake.ssn(), Sex.male if ismale else Sex.female,
                      fake.date_time_between_dates(date(1940, 1, 1), date.today()).date(),
                      race, ethn)
    
    @staticmethod
    def rand(n=None):
        if n is None: return Person._rand()
        return [Person._rand() for _ in tqdm(range(n),'creating people')]

def typo(s):
    chars = list(s)
    i = 0
    while i < len(chars):
        if random.random() < .00033333:
            chars.insert(i, chars[i])
            i = i+1
        if random.random() < .00033333 and 0 <= ord(chars[i].lower()) - ord('a') < 26:
            chars[i] = chr(((ord(chars[i].lower()) - ord('a') + random.choice(range(1,25))) % 26) + ord('a'))
        if random.random() < .00033333:
            chars.pop(i)
            i = i-1
        i = i+1
    return ''.join(chars)

def create_population(n=100, twin_rate=.025):
    pop = Person.rand(int(n * (1-twin_rate)))
    twins = np.random.choice(pop, n - len(pop))
    for t in tqdm(twins, 'adding twins'):
        pop.append(t.twin())
    return pop

def create_csv(pop, filename, mean=20):
    with open(filename,'w') as fd:
        csvwriter = csv.writer(fd)
        csvwriter.writerow(['uuid','first_name','last_name','ssn','sex','dob','race','ethnicity'])
        for p in tqdm(pop, desc='writing csv'):
            for _ in range(int(random.expovariate(1/mean))+1):
                p.write_row(csvwriter)

if __name__ == '__main__':
    population = create_population(20000)
    create_csv(population, 'people.csv')
