import os
import glob
import pandas as pd
from dateutil.parser import parse
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import warnings
from concurrent.futures import as_completed
import warnings

def time_to_timestamp(time_str):
    dt = parse(str(time_str))
    return int(dt.timestamp() * 1000)


def process_file(file):
    print(f"Processing {file}")
    chunkno = 0

    
    filename = os.path.basename(file)
    new_filename = filename.rsplit('.', 2)[0] + '.pkl.gz'
    processed_chunks = []

    try:
        chunk_iterator = pd.read_json(file, compression='gzip', lines=True, chunksize=10000)
        for chunk in chunk_iterator:
            chunkno += 1
            newdf = pd.DataFrame(index=chunk.index)

            newdf['user.id_str'] = chunk['user'].apply(lambda x: x.get('id_str') if isinstance(x, dict) else pd.NA)
            newdf['user.screen_name'] = chunk['user'].apply(lambda x: x.get('screen_name') if isinstance(x, dict) else pd.NA)
            newdf['in_reply_to_user_id_str'] = chunk['in_reply_to_user_id_str'].apply(lambda x: int(x) if pd.notnull(x) else pd.NA)
            newdf['in_reply_to_screen_name'] = chunk['in_reply_to_screen_name']
            newdf['retweeted_status.user.id_str'] = chunk['retweeted_status'].apply(lambda x: x.get('user', {}).get('id_str', pd.NA) if isinstance(x, dict) else pd.NA)
            newdf['retweeted_status.user.name'] = chunk['retweeted_status'].apply(lambda x: x.get('user', {}).get('screen_name', pd.NA) if isinstance(x, dict) else pd.NA)
            newdf['quoted_status.user.id_str'] = chunk['quoted_status'].apply(lambda x: x.get('user', {}).get('id_str', pd.NA) if isinstance(x, dict) else pd.NA)
            newdf['quoted_status.user.name'] = chunk['quoted_status'].apply(lambda x: x.get('user', {}).get('screen_name', pd.NA) if isinstance(x, dict) else pd.NA)
            newdf['place_full_name'] = chunk['place'].apply(lambda x: x.get('full_name') if isinstance(x, dict) else pd.NA)
            newdf['place_country'] = chunk['place'].apply(lambda x: x.get('country_code') if isinstance(x, dict) else pd.NA)
            newdf['lang'] = chunk['lang']
            newdf['timestamp_ms'] = chunk['timestamp_ms'].apply(time_to_timestamp)
            newdf['hashtags'] = chunk.apply(
                lambda x: (
                    [hashtag.get('text') for hashtag in x['extended_tweet'].get('entities', {}).get('hashtags', [])]
                    if isinstance(x.get('extended_tweet'), dict) and x['extended_tweet']
                    else [hashtag.get('text') for hashtag in x.get('entities', {}).get('hashtags', [])]
                ) or pd.NA, axis=1)

            processed_chunks.append(newdf)

        final_df = pd.concat(processed_chunks, ignore_index=True)
        output_path = os.path.join('RUSSIA/inside/', new_filename)
        final_df.to_pickle(output_path, compression='gzip')
        del final_df
        return file
    except Exception as e:
        print(f"Error processing {file}: {e}")
        return None


if __name__ == "__main__":

    warnings.simplefilter(action='ignore', category=FutureWarning)

    json_folder = 'RUSSIA/JSON/'
    json_files = glob.glob(os.path.join(json_folder, "*.json.gz"))


    num_workers = 4

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in json_files}

        results = []
        for future in as_completed(futures):
            file = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Finished: {file}")
            except Exception as e:
                print(f"Error in file {file}: {e}")

    print("Completed files:")
    print([r for r in results if r])