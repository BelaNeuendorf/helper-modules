import json

def file_len(fname):
    with open(fname, 'r') as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except (TypeError, OverflowError):
        return False
    
    
def get_link_ids_with_prefix(path_to_file):
    link_ids =  set()
    len_file = file_len(path_to_file)
    
    if len_file == 1:
        with open(path_to_file, 'r') as file:
            for line in file:
                d = json.loads(line)
                for comment in d:
                    l_id = comment['link_id']
                    link_ids.add(l_id)
                    
    if len_file > 1:
        with open(path_to_file, 'r') as file:
            one_string = ''
            for line in file:
                one_string += line
                
            d = json.loads(one_string)
            for comment in d:
                l_id = comment['link_id']
                link_ids.add(l_id)
                    
    link_ids = list(link_ids)
    return link_ids

def get_link_ids_without_prefix(path_to_file):
    link_ids = get_link_ids_with_prefix(path_to_file)
    return [l[3:] for l in link_ids]

