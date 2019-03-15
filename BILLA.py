
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.mlab as mlab
import os
import numpy as np


class Analysis:
    def __init__(self):
        self.db_host = ''
        self.db_name = ''
        self.db_port = 27017
        self.db_user = 'app'
        self.db_pwd = ''
        self.db_report_name = 'report'
        self.db_ads_name = 'ads'
        self.client = None
        self.db = None
        self.behaviors_attribute = dict()
        self.interests_attribute = dict()
        self.locations_attribute = dict()

    def mongodb_conn(self):
        client = MongoClient(host=self.db_host, port=self.db_port)
        db = client.get_database(self.db_name)
        db.authenticate(self.db_user.strip(), self.db_pwd.strip())
        self.client = client
        self.db = db

    def client_close(self):
        self.client.close()

    def find_ads(self, ad_ids):
        colles_ads = self.db.get_collection(self.db_ads_name).find({'ad_id':{'$in':list(ad_ids.keys())}},
                                                                   {'_id': 0, 'ad_id': 1,
                                                                    'pt.adset_spec.targeting.behaviors': 1,
                                                                    'pt.adset_spec.targeting.interests': 1,
                                                                    'pt.adset_spec.targeting.geo_locations.custom_locations': 1,
                                                                    })
        for ads in colles_ads:
            pt = ads['pt']
            if pt.get('adset_spec') and pt['adset_spec'].get('targeting'):
                if pt['adset_spec']['targeting'].get('behaviors'):
                    behaviors = pt['adset_spec']['targeting']['behaviors']
                    if isinstance(behaviors,list):
                        for behavior in behaviors:
                            if behavior['id'] not in self.behaviors_attribute:
                                self.behaviors_attribute[behavior['id']] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.behaviors_attribute[behavior['id']]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.behaviors_attribute[behavior['id']]['size'] += 1
                    elif isinstance(behaviors, dict):
                        for behavior in behaviors.values():
                            if behavior['id'] not in self.behaviors_attribute:
                                self.behaviors_attribute[behavior['id']] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.behaviors_attribute[behavior['id']]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.behaviors_attribute[behavior['id']]['size'] += 1

                if pt['adset_spec']['targeting'].get('interests'):
                    interests = pt['adset_spec']['targeting']['interests']
                    if isinstance(interests,list):
                        for interest in interests:
                            if interest['id'] not in self.interests_attribute:
                                self.interests_attribute[interest['id']] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.interests_attribute[interest['id']]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.interests_attribute[interest['id']]['size'] += 1
                    elif isinstance(interests,dict):
                        for interest in interests.values():
                            if interest['id'] not in self.interests_attribute:
                                self.interests_attribute[interest['id']] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.interests_attribute[interest['id']]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.interests_attribute[interest['id']]['size'] += 1

                if pt['adset_spec']['targeting'].get('geo_locations') and pt['adset_spec']['targeting']['geo_locations'].get('custom_locations'):
                    locations = pt['adset_spec']['targeting']['geo_locations']['custom_locations']
                    if isinstance(locations, list):
                        for location in locations:
                            ll = str(round(float(location['latitude'])))+'_'+str(round(float(location['longitude'])))
                            if ll not in self.locations_attribute:
                                self.locations_attribute[ll] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.locations_attribute[ll]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.locations_attribute[ll]['size'] += 1
                    elif isinstance(locations, dict):
                        for location in locations.values():
                            ll = str(round(float(location['latitude'])))+'_'+str(round(float(location['longitude'])))
                            if ll not in self.locations_attribute:
                                self.locations_attribute[ll] = {'cost': 0, 'size': ad_ids[ads['ad_id']]['size']}
                            self.locations_attribute[ll]['cost'] += ad_ids[ads['ad_id']]['cost']
                            self.locations_attribute[ll]['size'] += 1

    def find_reports(self):
        colles_reports = self.db.get_collection(self.db_report_name).find({}, {'_id': 0, 'ad_id': 1, 'cost': 1})
        ad_ids = {}
        for report in colles_reports:
            if report['ad_id'] not in ad_ids:
                ad_ids[report['ad_id']] = {'cost':0, 'size': 0}
            ad_ids[report['ad_id']]['cost'] += report['cost']
            ad_ids[report['ad_id']]['size'] += 1
        self.find_ads(ad_ids)

    def find_ads_attribute_lens(self, ad_ids):
        colles_ads = self.db.get_collection(self.db_ads_name).find({'ad_id': {'$in': ad_ids}},
                                                                   {'_id': 0, 'ad_id': 1,
                                                                    'pt.adset_spec.targeting.behaviors': 1,
                                                                    'pt.adset_spec.targeting.interests': 1,
                                                                    'pt.adset_spec.targeting.geo_locations.custom_locations': 1,
                                                                    })
        print(colles_ads.count())
        for ads in colles_ads:
            pt = ads['pt']
            aid = ads['ad_id']
            if pt.get('adset_spec') and pt['adset_spec'].get('targeting'):
                if pt['adset_spec']['targeting'].get('behaviors'):
                    behaviors = pt['adset_spec']['targeting']['behaviors']
                    if aid not in self.behaviors_attribute:
                        self.behaviors_attribute[aid] = {'lens': 0, 'size': 0}
                    self.behaviors_attribute[aid]['lens'] += len(behaviors)
                    self.behaviors_attribute[ads['ad_id']]['size'] += 1

                if pt['adset_spec']['targeting'].get('interests'):
                    interests = pt['adset_spec']['targeting']['interests']
                    if aid not in self.interests_attribute:
                        self.interests_attribute[aid] = {'lens': 0, 'size': 0}
                    self.interests_attribute[aid]['lens'] += len(interests)
                    self.interests_attribute[aid]['size'] += 1

                if pt['adset_spec']['targeting'].get('geo_locations') and pt['adset_spec']['targeting']['geo_locations'].get('custom_locations'):
                    locations = pt['adset_spec']['targeting']['geo_locations']['custom_locations']
                    if aid not in self.locations_attribute:
                        self.locations_attribute[aid] = {'lens': 0, 'size': 0}
                    self.locations_attribute[aid]['lens'] += len(locations)
                    self.locations_attribute[aid]['size'] += 1

    def find_reports_attribute_lens(self):
        colles_reports = self.db.get_collection(self.db_report_name).find({},
                                                                          {'_id': 0, 'ad_id': 1, 'cost': 1})
        ad_ids = list()
        for report in colles_reports:
            ad_ids.append(report['ad_id'])
        self.find_ads_attribute_lens(ad_ids)

    def plot_attribute(self):
        with open('attribute_lens.csv', 'a+') as fopen:
            fopen.write('ad_id,size,lens,category\n')
            for k, v in self.behaviors_attribute.items():
                fopen.write(str(k)+','+','+str(v['size'])+','+str(v['lens'])+',behavior\n')
            for k, v in self.interests_attribute.items():
                fopen.write(str(k)+','+','+str(v['size'])+','+str(v['lens'])+',interest\n')
            for k, v in self.locations_attribute.items():
                fopen.write(str(k)+','+','+str(v['size'])+','+str(v['lens'])+',location\n')
        print(len(self.behaviors_attribute), len(self.interests_attribute), len(self.locations_attribute))

        data = pd.read_csv('attribute_lens.csv')
        self.behaviors_attribute = data[data['category'] == 'behavior']
        self.interests_attribute = data[data['category'] == 'interest']
        self.locations_attribute = data[data['category'] == 'location']
        print('plot show')

        # plt.subplot(211)
        # plt.title('behaviors')
        # plt.ylabel('size')
        # print(self.behaviors_attribute.columns)
        # blst = [0 for _ in range(max(self.behaviors_attribute['lens'])+1)]
        # for xx in self.behaviors_attribute['lens']:
        #     blst[xx] += 1
        # plt.plot([i for i in range(len(blst))], blst, 'ro')
        #
        # plt.subplot(212)
        # plt.title('interests')
        # plt.ylabel('size')
        #
        # blst = [0 for _ in range(max(self.interests_attribute['lens'])+1)]
        # for xx in self.interests_attribute['lens']:
        #     blst[xx] += 1
        # plt.plot([i for i in range(len(blst))], blst, 'yo')
        # plt.subplot(313)
        # plt.title('location')
        # plt.ylabel('size')
        #
        # blst = [0 for _ in range(max(self.locations_attribute['lens'])+1)]
        # for xx in self.locations_attribute['lens']:
        #     blst[xx] += 1
        # plt.plot([i for i in range(len(blst))], blst, 'go')
        #
        # plt.show()
        #
        # plt.subplot(211)
        # plt.title('behaviors filter 1')
        # plt.ylabel('lens')
        # self.behaviors_attribute.sort_values(by=['lens'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.behaviors_attribute))], self.behaviors_attribute['lens'], 'ro')
        #
        # plt.subplot(212)
        # plt.title('interests filter 1')
        # plt.ylabel('lens')
        # self.interests_attribute.sort_values(by=['lens'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.interests_attribute))], self.interests_attribute['lens'], 'yo')
        #
        # plt.subplot(313)
        # plt.title('location filter 1')
        # plt.ylabel('lens')
        # self.locations_attribute.sort_values(by=['lens'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.locations_attribute))], self.locations_attribute['lens'], 'go')
        #
        # plt.show()

        plt.subplot(311)
        plt.title('behaviors')
        # plt.ylabel('lens')
        self.behaviors_attribute = data[data['category'] == 'behavior']
        n, bins, patches = plt.hist(self.behaviors_attribute['lens'], 5, alpha=0.5)
        print(n, bins, patches)
        # tmp = [[] for _ in range(N)]
        # for behavior in self.behaviors_attribute['lens']:
        #     for index in range(len(n)):
        #         if n[index] < self.behaviors_attribute['lens']:

        plt.subplot(312)
        plt.title('interests')
        # plt.ylabel('lens')
        self.interests_attribute = data[data['category'] == 'interest']
        plt.hist(self.interests_attribute['lens'], 5, alpha=0.5)

        plt.subplot(313)
        plt.title('location filter 1')
        plt.ylabel('lens')
        self.locations_attribute = data[data['category'] == 'location']
        plt.hist(self.locations_attribute['lens'], 20, alpha=0.5)

        plt.show()

    def analysis_interests(self):

        self.interests_attribute['mean_cost'] = self.interests_attribute['cost']/self.interests_attribute['size']

        # pd.DataFrame().sort_values()
        self.interests_attribute.sort_values(by=['cost'], ascending=False, inplace=True)
        self.interests_attribute['rank_cost'] = np.arange(1, len(self.interests_attribute)+1)
        tmp_lst1 = set(self.interests_attribute['name'][:4000])
        print(tmp_lst1)
        print(len(tmp_lst1))
        self.interests_attribute.sort_values(by=['mean_cost'], ascending=False, inplace=True)
        self.interests_attribute['rank_mean'] = np.arange(1, len(self.interests_attribute)+1)
        tmp_lst2 = set(self.interests_attribute['name'][:4000])
        self.interests_attribute['rank'] = 0.5*self.interests_attribute['rank_cost'] + 0.5*self.interests_attribute['rank_mean']
        self.interests_attribute.sort_values(by=['rank'], ascending=True, inplace=True)
        self.interests_attribute.to_csv('analysis_interests.csv', index=False)
        tmp_lst3 = set(self.interests_attribute['name'][:4000])
        # plt.subplot(211)
        # plt.plot(np.arange(len(self.interests_attribute)),self.interests_attribute['mean_cost'], r'b.')
        # plt.subplot(212)
        # plt.plot(np.arange(len(self.interests_attribute)),self.interests_attribute['cost'], r'g.')
        # plt.show()
        # print(self.interests_attribute)
        # print(len(self.interests_attribute))
        pd.DataFrame({'id': list(tmp_lst3)}).to_csv('top4000_interest.csv', index=False)
        print('cost & mean_cost', 4000-len(tmp_lst1 & tmp_lst2))
        print('cost & rank', 4000-len(tmp_lst1 & tmp_lst3))
        print('mean_cost & rank', 4000-len(tmp_lst2 & tmp_lst3))
        print('cost & mean_cost & rank', 4000-len(tmp_lst1 & tmp_lst2 & tmp_lst3))

    def plot(self):
        # with open('attribute.csv','a+') as fopen:
        #     fopen.write('name,cost,size,category\n')
        #     for k, v in self.behaviors_attribute.items():
        #         fopen.write(str(k)+','+str(v['cost'])+','+str(v['size'])+',behavior\n')
        #     for k, v in self.interests_attribute.items():
        #         fopen.write(str(k)+','+str(v['cost'])+','+str(v['size'])+',interest\n')
        #     for k, v in self.locations_attribute.items():
        #         fopen.write(str(k)+','+str(v['cost'])+','+str(v['size'])+',location\n')
        data = pd.read_csv('attribute.csv')
        self.behaviors_attribute = data[data['category'] == 'behavior']
        self.interests_attribute = data[data['category'] == 'interest']
        self.locations_attribute = data[data['category'] == 'location']
        print(len(self.behaviors_attribute), len(self.interests_attribute), len(self.locations_attribute))

        self.analysis_interests()
        # plt.subplot(311)
        # plt.title('behaviors')
        # plt.ylabel('size')
        # self.behaviors_attribute.sort_values(by=['size'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.behaviors_attribute))], self.behaviors_attribute['size'],'ro')
        #
        # plt.subplot(312)
        # plt.title('interests')
        # plt.ylabel('size')
        # self.interests_attribute.sort_values(by=['size'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.interests_attribute))], self.interests_attribute['size'], 'go')
        #
        # plt.subplot(313)
        # plt.title('location')
        # plt.ylabel('size')
        #
        # self.locations_attribute.sort_values(by=['size'], ascending=False, inplace=True)
        # plt.plot([i for i in range(len(self.locations_attribute))], self.locations_attribute['size'], 'yo')
        # plt.show()

    def amain(self):
        self.find_reports_attribute_lens()
        self.plot_attribute()

    def tmain(self):
        self.find_reports()
        self.plot()

    def main(self):
        self.mongodb_conn()
        self.amain()
        self.client_close()


if __name__ == '__main__':
    analysis = Analysis()
    analysis.main()
