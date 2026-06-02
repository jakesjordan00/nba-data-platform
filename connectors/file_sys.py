
import sys
import os
import logging
import requests
from pathlib import Path


class FileSystem:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.file_sys')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.file_sys')
        self.root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._set_library_directories()
        bp = 'here'



    def _set_library_directories(self):
        '''`_set_library_directories`(self)
        ---
        <hr>
        
        Checks for directories needed to save TrailCam media, creates them if they do not already exist
        
        <hr>
        
        Sets
        ---        
        - #### self.:attr:`~library`: parent folder containing media
        '''
        bp = 'here'
        self.nba_videos = Path.home() / 'Documents' / '.nba-videos'
        self._create_directory(self.nba_videos)
        


    def _create_directory(self, path: Path):
        '''`_create_directory`(self, path: *Path*)
        ---
        <hr>

        Creates directory at the path specified if it does not already exist

        <hr>

        Parameters
        ---
        :param (*Path*) `path`: Path in which directory should be created if not already existing

        <hr>

        Returns
        ---
        '''
        # print(f"Creating {path.name} folder in {path.parent} if it doesn't already exist")
        path.mkdir(parents=True, exist_ok=True)



    def list_files(self, path: Path) -> list[Path]:
        '''`list_files`(self, path: *Path*)
        ---
        <hr>

        Returns a list of files (excluding sub-directories) within the folder at the specified path

        <hr>

        Parameters
        ---
        :param (*Path*) `path`: Path of the folder to list files from

        <hr>

        Returns
        ---
        :return (*list[Path]*): List of paths to files within the folder
        '''
        path = Path(path)
        files = [child for child in path.iterdir() if child.is_file()]
        return [child for child in path.iterdir() if child.is_file()]

    def list_all_files(self, path: Path | None = None) -> list[Path]:
        '''`list_files`(self, path: *Path*)
        ---
        <hr>

        Returns a list of files (***including*** sub-directories) within the folder at the specified path

        <hr>

        Parameters
        ---
        :param (*Path*) `path`: Path of the folder to list files from

        <hr>

        Returns
        ---
        :return (*list[Path]*): List of paths to files within the folder
        '''
        path = self.nba_videos if not path else path
        path = Path(path)
        files = [child for child in Path(path).rglob(pattern = '*') if child.is_file()] 
        return files



    def _find_existing(self, stem: str, directory: Path) -> Path | None:
        '''Returns the first file matching `stem` (any extension) across the directory passed'''
        for d in (directory):
            match = next((f for f in self.list_files(d) if f.stem == stem), None)
            if match:
                return match
        return None


    def download_videos(self, actions: dict[tuple, dict]):
        '''`download_videos`(actions: *dict[tuple, dict]*, )
        ---
        <hr>
        
        put_summary_here
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*dict[tuple, dict]*) `actions`: dictionary with a tuple as a key and a dict as the value

        - **Key**: *`(row['GameID'], row['ActionNumber'])`*
            - This translates to something that may look like (22500123, 24)
        - **Value**: 
        >>>     {
                    'PlayerID':  row['PlayerID'],
                    'Description': row['Description'],
                    'PointInGame': row['PointInGame'],
                    'Qtr': row['Qtr'],
                    'Clock': row['Clock'],
                    'params': {
                        'GameID': f'00{row['GameID']}',
                        'GameEventID': row['ActionNumber']
                    },
                }

        ***which will look like the following at execution***

        >>>     {
                    'PlayerID': 203497, 
                    'Description': 'MISS R. Gobert Free Throw 2 of 2', 
                    'PointInGame': Decimal('13.38'), 
                    'Qtr': 1, 
                    'Clock': '05:35.00', 
                    'params': {'GameID': '0022500044', 'GameEventID': 90}
                }



        
        <hr>
        
        Returns
        ---
        '''
        total = len(actions.keys())
        for i, ((GameID, ActionNumber), data) in enumerate(actions.items()):
            file_name = f'{GameID}-{ActionNumber}.mp4'
            prefix = f'{GameID}-{ActionNumber}: '
            suffix = f'{i+1}/{total}'
            try:
                self.logger.info(f'{prefix}Retrieving video information...{suffix}')
                response = requests.get(self.pipeline.base_url, params=data['params'], headers=self.pipeline.headers, timeout=30)
                data = response.json()
            except Exception as e:
                self.logger.error(f'{prefix}Error parsing initial request json! {e}')
                continue
            try:
                self.logger.info(f'{prefix}Successfully retreived video information! Parsing URL...')
                video_url = data['resultSets']['Meta']['videoUrls'][0]['lurl']
            except Exception as e:
                self.logger.error(f'{prefix}Error parsing URL! {e}')
                continue            
            try:
                self.logger.info(f'{prefix}Successfully parsed video URL, attempting download...')
                video_response = requests.get(video_url, stream=True)
                video_response.raise_for_status()
            except Exception as e:
                self.logger.error(f'{prefix}Error downloading! {e}')
                continue
            try:
                self.logger.info(f'{prefix}Successfully downloaded video! Now saving...')
                self._finalize_video_download(video_response, file_name)
            except Exception as e:
                self.logger.error(f'{prefix}Error saving video! {e}')
                continue

        bp = 'here'



    def _finalize_video_download(self, video_response, file_name):
        output_path = self.nba_videos / file_name
        self.logger.info(f'Attempting save to {output_path}...')
        try:
            with open(output_path, 'wb') as f:
                for chunk in video_response.iter_content(chunk_size=8192):
                    f.write(chunk)
        except Exception as e:
            self.logger.error(f'{file_name} could not be saved!')
            return
        self.logger.info(f'{file_name} saved successfully!')
        


    def download(self, media: list[dict], do_upsert: bool = False):
        '''NOT FOR USE IN THIS PROJECT YET, USE :meth:`~download_videos`
        Downloads every new image referenced in `media`, converting HEIC/HEIF to PNG en route.
        '''
        sql_list = []
        for i, item in enumerate(media):
            file_name = item['FileName']
            staging_path = Path(item['DownloadStagingPath'])
            dest = Path(item['Path'])
            self.logger.info(f'downloading {file_name} to {staging_path.name}, {len(media) - i} images to go')
            try:
                r = requests.get(item['URL'], stream=True)
                r.raise_for_status()
                with open(staging_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        f.write(chunk)
                self._finalize_download(item, staging_path, dest)
                item['Downloaded'] = 1
                if do_upsert:
                    sql_list.append(item)
                    if (i % 10 == 0 and i != 0) or i == len(media) - 1:
                        self.pipeline.sql_db.checked_upsert(table_name='MediaItems', data=sql_list)
                        sql_list.clear()
                        bp = 'here'
            except Exception as e:
                self.logger.error(f'Error downloading {file_name}: {e}')
        return media




