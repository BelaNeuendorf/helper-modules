import sys
import pickle as pkl
import pandas as pd
import time
import datetime
import logging
from psaw import PushshiftAPI
import praw
import random
import math
import os
import glob
import json
from zipfile import ZipFile
import enlighten
from .utils import *
from pathlib import Path
from tqdm import tqdm
from prawcore.exceptions import ResponseException

class RedditCrawlPipeline():
    # do you want that the intermediate result during crawling is stored after every 100 submissions? 
    # then use_caching = True
    use_caching = False

    # should the comments of the submissions be crawled as well (and be stored in a seperate file)?
    crawl_comments = False

    # should the subreddits of the submissions be crawled as well (and be stored in a seperate file)?
    crawl_subreddits = False
    
    # Input arguments
    input_file_path = None
    credentials_file_path = None
    output_dir_path = None
    failed_log_file = None
    api = None
    
    # get selected attributes for submissions and comments from input file
    submissions_selected_attributes = []
    comments_selected_attributes = []

    def __init__(self, configs):
        if type(configs) == dict:
            config_dict = configs
        elif type(configs) == str:
            assert configs.split('.')[-1]=='json', 'config file needs to be the path of a json file'
            with open(configs, 'r') as f:
                config_dict = json.load(f)
        else:
            raise Exception('submission ids are not a list or a path to a file')
        
        for key in config_dict:
            setattr(self, key, config_dict[key])
        self._handle_credentials()
        self._setup_output_dir()
        self.failed_log_file = self.output_dir_path+"failed_craws_log.txt"

    def _setup_output_dir(self):
        if os.path.exists(self.output_dir_path):
            raise ValueError('OUTPUT DIRECTORY ALREADY EXISTS: Will not override. Please provide a path to empty directory.')
        if not self.output_dir_path[-1] == '/':
            raise ValueError('OUTPUT PATH IS NOT A DIRECTORY FORMAT:  Please provide a path ending with a forward slash ('/').')
        os.mkdir(self.output_dir_path)
        
    def _handle_credentials(self):
        with open(self.credentials_file_path) as file:
            credentials_dict = json.load(file)
                         
            self.client_id = credentials_dict['client_id']
            self.client_secret = credentials_dict['client_secret']
            self.user_agent = credentials_dict['user_agent']
        
        self.api = praw.Reddit(client_id=self.client_id,
                          client_secret=self.client_secret,
                          user_agent=self.user_agent)
        try:
            self.api.random_subreddit()
        except ResponseException as e:
            raise Exception('\n\nPRAW API credential seem to be incorrect, check if the correct ones are set in credentials.json\n\n')
                 
    def crawl(self, submission_ids=None):
        print('Crawling Submissions (and comments if selected) now:')
        if not submission_ids:
            if self.input_file_path:
                submission_ids = self.input_file_path
            else:
                raise Exception('No list of ids of path to text file containing ids provided.')
                
        if type(submission_ids) == list:
            submission_ids = process_ids(submission_ids)
        elif type(submission_ids) == str:
            submission_ids = read_and_process_ids(submission_ids)
        else:
            raise Exception('submission ids are not a list or a path to a file')

        # the api allows to make 100 calls at once.
        len_ids = len(submission_ids)
        num_iterations = math.ceil(len_ids/100)

        # the results are stored here
        all_submissions_df = pd.DataFrame()
        all_comments_df = pd.DataFrame()

        # set up logging
        t = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
        logfile_name = f"crawling_log_started_at_{t}.log"
        logging.basicConfig(filename= self.output_dir_path+logfile_name, level=logging.DEBUG)

        # craw submissions, each iteration 100 at once
        for k in tqdm(range(num_iterations)):

            # ranges
            left = k*100
            right = min((k+1)*100, len_ids)

            #loggging
            logging.info('_'*100)
            logging.info(f"Processing steps {left} to {right}")
            logging.info(str(datetime.datetime.now()))
            logging.info('_'*100)

            # API call
            try:
                api_submissions = list(self.api.info(submission_ids[left:right]))
            except Exception as e:
                logging.info('ERROR AT API:' + ('-'*80))
                logging.info(str(e.__class__))
                logging.info(str(e))
                logging.info('-'*100)
                self.write_failed_log(f"Failed IDs at itteration {k}:\n{str(submission_ids[left:right])}\n")
                continue

            # parse data, store result
            if self.crawl_comments and bool(api_submissions):
                submissions_df, comments_df = self.parse_submissions(api_submissions, 
                                                                logfile_name, 
                                                                self.crawl_comments, 
                                                                self.submissions_selected_attributes,
                                                                self.comments_selected_attributes)
                all_submissions_df = pd.concat([all_submissions_df,submissions_df],ignore_index=True)
                all_comments_df = pd.concat([all_comments_df,comments_df],ignore_index=True)
            elif bool(api_submissions):
                submissions_df = self.parse_submissions(api_submissions, 
                                                   logfile_name, 
                                                   self.crawl_comments,
                                                   self.submissions_selected_attributes, 
                                                   self.comments_selected_attributes)
                all_submissions_df = pd.concat([all_submissions_df,submissions_df],ignore_index=True)

            # store the intermediate result each iteration in case an error happens during running
            if self.use_caching:
                all_submissions_df.to_pickle(self.output_dir_path+'submissions_CACHED.gzip',compression='gzip')
                if self.crawl_comments:
                    all_comments_df.to_pickle(self.output_dir_path+'comments_CACHED.gzip',compression='gzip')

        #store result
        all_submissions_df.to_pickle(self.output_dir_path+'submissions.gzip',compression='gzip')
        if self.crawl_comments:
            all_comments_df.to_pickle(self.output_dir_path+'comments.gzip',compression='gzip')

        # crawl subreddits
        if self.crawl_subreddits:
            print('Crawling subreddits now:')
            subreddit_names = list(set(all_submissions_df.subreddit_name_prefixed))
            subreddit_names = [name[2:] if (name and name[:2]=='r/') else name for name in subreddit_names]

            # the results are stored here
            all_subreddits_df = pd.DataFrame()

            # set up logging
            t = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
            logfile_name = f"crawling_subreddits_log_started_at_{t}.log"
            logging.basicConfig(filename=self.output_dir_path+logfile_name, level=logging.DEBUG)

            # craw subreddits
            for sub_name in tqdm(subreddit_names):               

                #loggging
                logging.info('_'*100)
                logging.info(f"CRAWLING SUBREDDIT: {sub_name}")
                logging.info(str(datetime.datetime.now()))
                logging.info('_'*100)

                # API call
                try:
                    api_subreddit = self.api.subreddit(sub_name)
                except Exception as e:
                    logging.info('ERROR AT API FOR SUBREDDIT:' + ('-'*80))
                    logging.info(str(e.__class__))
                    logging.info(str(e))
                    logging.info('-'*100)
                    continue

                try:
                    api_subreddit.name # again this is done to make the api fetch all data 
                except Exception as e:
                    fail_message = f'Failed retrieving subreddit: {sub_name}\n{str(e)}'
                    self.write_failed_log(fail_message)
                    logging.info('ERROR AT CRAWLING SUBREDDIT - wrote to failed-log' + ('-'*70))
                    logging.info(str(e.__class__))
                    logging.info(str(e))
                    logging.info('-'*100)
                    
                    
                subreddit_dict = api_subreddit.__dict__

                all_subreddits_df = pd.concat(
                    [all_subreddits_df, pd.DataFrame.from_records([subreddit_dict])],
                    ignore_index=True)

            #store result
            all_subreddits_df.to_pickle(self.output_dir_path+'subreddits.gzip',compression='gzip')
            
                   
    def parse_author(self, author, post_id, is_comment):
        result = dict()
        if not author:
            result['author_name'] = 'deleted'
            result['author_id'] = 'deleted'
        else:
            try:
                result['author_name'] = author.name
                result['author_id'] = author.id
            except Exception as e:
                try:
                    if author.is_suspended:
                        result['author_name'] = 'deleted'
                        result['author_id'] = 'deleted'
                    else:
                        raise Exception(str(e))
                except Exception as e:
                    result['author_name'] = None
                    result['author_id'] = None
                    if not is_comment:
                        fail_message = f'Failed extracting submissions author:\nSUBMISSION_ID={post_id}\n{str(e)}'
                    else:
                        fail_message = f'Failed extracting comments author:\nCOMMENT_ID={post_id}\n{str(e)}'
                    self.write_failed_log(fail_message)
                    logging.info('ERROR AT PARSING COMMENT - wrote to failed-log' + ('-'*70))
                    logging.info(str(e.__class__))
                    logging.info(str(e))
                    logging.info('-'*100)
                    

        return result


    def parse_comment(self, comment, comments_selected_attributes):
        result = dict()
        comment_dict = comment.__dict__
        exclude_not_selected_attributes = [a for a in comment_exclude_attributes if a not in comments_selected_attributes]
        # first jsonables
        result.update({k:v for k,v in comment_dict.items() if (is_jsonable(v)
                                                               and k in (comments_selected_attributes or all_comment_attributes) # in case comments_selected_attributes is empty
                                                               and k not in handle_extra_comments_attributes + exclude_not_selected_attributes)})
        # now everything that is not jsonable and is extra handled
        result.update({k:str(v) for k,v in comment_dict.items() if (not is_jsonable(v) 
                                                                    and k in comments_selected_attributes
                                                                    and k not in exclude_not_selected_attributes)})

        if 'author' in comments_selected_attributes:
            result.update(self.parse_author(comment.author, comment.id, is_comment=True))
        replies_ids = [r.id for r in comment.replies]
        result['replies_ids'] = replies_ids

        return result


    def parse_comments(self, comments_list, comments_selected_attributes):
        result_list = []
        for comment in comments_list: 
            if type(comment) == praw.models.reddit.comment.Comment:
                result_list.append(self.parse_comment(comment, comments_selected_attributes))

            elif type(comment) == praw.models.reddit.more.MoreComments:
                more_comments = comment.comments()
                result_list.extend(self.parse_comments(more_comments, comments_selected_attributes))

        return result_list


    def parse_submission(self, submission, submissions_selected_attributes):
        result = dict()
        submission.title # without this just some default data is loaded for the submission instead of all
        submission.selftext
        submission_dict = submission.__dict__

        exclude_not_selected_attributes = [a for a in submission_exclude_attributes if a not in submissions_selected_attributes]
        # first jsonables
        result.update({k:v for k,v in submission_dict.items() 
                       if (is_jsonable(v) 
                           and k in (submissions_selected_attributes 
                                     or all_submission_attributes) # if not submissions_selected_attributes
                           and k not in handle_extra_submission_attributes + exclude_not_selected_attributes)})

        # now everything that is not jsonable and is extra handled
        result.update({k:str(v) for k,v in submission_dict.items() 
                       if (is_jsonable(v) 
                           and k in submissions_selected_attributes
                           and k not in exclude_not_selected_attributes)})

        if 'author' in submissions_selected_attributes:
            author = submission.author
            result.update(self.parse_author(author, submission.id, is_comment=False))

        return result
    

    def parse_submissions(self, submissions, 
                          logfile_name, 
                          crawl_comments, 
                          submissions_selected_attributes, 
                          comments_selected_attributes):
        logging.basicConfig(filename=logfile_name, level=logging.DEBUG)
        submissions_df = pd.DataFrame()
        comments_df = pd.DataFrame()

        for submission in submissions:
            sub_dict = self.parse_submission(submission, submissions_selected_attributes)
            submissions_df = pd.concat(
                    [submissions_df, pd.DataFrame.from_records([sub_dict])],
                    ignore_index=True)

            if crawl_comments:
                comment_list = submission.comments.list()
                comment_dicts_list = self.parse_comments(comment_list, comments_selected_attributes)
                comments_df = pd.concat(
                    [comments_df, pd.DataFrame.from_records(comment_dicts_list)],
                    ignore_index=True)

                return submissions_df, comments_df

        return submissions_df
    
    def write_failed_log(self, text):
        with open(self.failed_log_file, 'w') as file:
            file.write('\n' + '_'*100 + '\n')
            file.write(text)
