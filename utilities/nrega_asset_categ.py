category2keyword = {
    'Irrigation - Site level impact': ['bund', 'bandh', 'बांध', 'बांध', 'बॉध',
                                       'tank',
                                       'irrigation',
                                       'well-filter', 'koop-filter', 'कुऑ-filter', 'कूप-filter', 'कुप-filter', 'kup-filter',
                                       'तालाब-fish', 'pond-fish', 'dobha-fish', 'talab-fish', 'pokhar-fish', 'pokhra-fish', 'पोखर-fish',
                                       'percolation',
                                       'desilting',
                                       'sichai kup', 'sinchai kup', 'सिंचाई कूप',
                                       'fp of',
                                       'nali nirman', 'नाली निर्माण',
                                       ],
    'SWC - Landscape level impact': ['aahar', 'ahhar', 'ahar', 'अहार', 'अहर', 'आहार', 'आहर',
                                     'sarkari', 'government',
                                     'anicut', 'dam', 'चेकडेम',
                                     'terrace', 'trench', 'diversion channel', 'diversion drain', 'diversion',
                                     'drain-plantation', 'gabion', 'bench', 'plug', 'canal-plantation', 'नाली', 'नाला-plantation', 'pit-compost',
                                     'channel', 'embank', 'drain-drainage', 'dyke', 'watercourse', 'soak', 'spur'
                                     'whs', 'w.h.s.',
                                     'silviculture',
                                     'excavation',
                                     'sokhata nirman',
                                     'reclamation land', #'reclamation of land',
                                     'loose bolder', 'loose boulder', # 'loose bolder structure'
                                     ],


    'Plantation': ['plantation', 'palantation', 'plantaion', 'वृक्षारोपण', 'वक्षारोपण', 'briksha', 'vriksharopan', 'brixaropan', 'brakharopn', 'वृक्षा',  'tree',
                   'forestry', 'nursery', 'forest', 'grass', 'farm forestri',
                   'afforestation', 'farm forestry', 'silvipasture', 'shelter belt', 'horticulture', 'sericulture'],
    

    'Household Livelihood': ['shelter', 'fishery pond', 'drying yard', 'cattle', 'goat',
                             'poultry', 'poltri',
                             'piggery', 'livestock', 'fish'],
                            

    'Agri Impact - HH,  Community': ['land levelling', 'land leveling', 'land development',
                                     'chaur land', 'compost pit', 'fallow land',
                                     'shaping land', 'nadep', 'storage build', 'waterlogged land', 'waste land', 'storage building',
                                     'berkeley', 'vermi', 'azola', 'storage', 'biomanure',
                                     'nalla', 'nallah', 'nulla',
                                     'bpg', 'b.pg', 'bp.g', 'bpg.', 'b.p.g', 'b.pg.', 'bp.g.', 'b.p.g.',
                                     'pmay-g', 'pmayg',
                                     'मिटटी भराई', 'मिटी भराई',
                                     'miti bharai',
                                     'keth samtlikarn', 'samtali karan', 'समतलीकारण',
                                     'karaha katai',
                                     ],

    'Others - HH, Community': ['cement concrete',
                               'kharanja', 'karanja',
                               'haat', 'field', 'mitti',
                               'anganwadi', 'aganbari', 'anganbadi', 'आगंवारी', 'आगनबाडी', 'aaganwadi',
                               'crematorium',
                               'toilet', 'latrine', 'showchalay', 'sauchalay', 'souchliya', 'शौचालय',
                               'shed', 'sed nirman', 'wall', 'diwal',
                               'dibar', 'kitchen', 'bhavan', 'murrarn','मोरम', 'moram', 'house', 'bitumen', 'gravel',
                               'road', 'सडक', 'रोड', 'sadak',
                               'सड्क', 'school', 'vidyalay', 'vidyalya', 'विदयालय',
                               'awaas', 'awaas', 'आवास', 'आवस', 'अवास', 'अवस',
                               'sewa kendra', 'seva kendra', 'seba kendra', 'sheva kendra', 'seva kendra',
                               'iay', 'i.a.y.', 'i.ay', 'ia.y', 'iay.', 'i.a.y', 'ia.y.', 'i.ay.',
                               'fencing', 'fence',
                               'awc',
                               'play ground',
                               'ihhl',
                               'public assets',
                               'पुलिया निर्माण', 'पुल निर्माण',
                               'brick soling', 'pcc',
                               'ईट सोलिंग',
                                'कलभट', 'kalbhat', 'kalabhat',
                                'rcc puliya', 'rcc',
                                'cement lining',
                                'building material', # 'production of building material',
                               'minors', 'sub minors',
                               'makan nirman',
                               'safai', 'saphai'
                               'chabutara', 'chhawar'
                               ],

    'Irrigation Site level - Non RWH': ['filter', 'boring']
}




import pandas as pd
import re

def remove_special_chars(line):
  if isinstance(line, float): return ''
  if isinstance(line, int): return ''
  line = line.replace('?', ' ')
#   line = line.replace('.', '')
  line = line.replace(',', ' ')
  line = line.replace('&', ' ')
  # line = line.replace('-', ' ') # example: de-silted
  line = line.replace('%', ' ')
  line = line.replace('@', ' ')
  line = line.replace('(', ' ')
  line = line.replace(')', ' ')
  line = line.replace('/', ' ')
#   line = line.replace('\', ' ')
  line = line.replace('_', ' ')
  line = line.replace('[', ' ')
  line = line.replace(']', ' ')
  line = line.replace('{', ' ')
  line = line.replace('}', ' ')
  line = line.replace('$', ' ')
  line = line.replace('#', ' ')
  line = line.replace('!', ' ')
  line = line.replace('^', ' ')
  line = line.replace('*', ' ')
  line = line.replace('+', ' ')
  line = line.replace('=', ' ')
  line = line.replace('|', ' ')
  line = line.replace(';', ' ')
  line = line.replace('<', ' ')
  line = line.replace('>', ' ')
  line = line.replace(':', ' ')
  line = re.sub(r'\d', ' ', line)
  line = line.strip()
  return line

def remove_short_words(line):
  stop_words = [
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
    "you", "your", "yours", "yourself", "yourselves",
    "he", "him", "his", "himself", "she", "her", "hers", "herself",
    "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
    "what", "which", "who", "whom", "this", "that", "these", "those",
    "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing",
    "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
    "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
    "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out",
    "on", "off", "over", "under", "again", "further", "then", "once",
    "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
    "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
    "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",

    "along", "continious", "for", "ior", "lor", "tor"
  ]
  words = line.split(' ')
  words = list(filter(lambda word : len(word)>2, words))
  words = list(filter(lambda word: word not in stop_words, words))
  line = ' '.join(words)
  return line

category2keyword = category2keyword

def clean(string):
    string = str(string)
    string = string.lower()
    string = remove_special_chars(string)
    string = remove_short_words(string)
    return string

def getKeywork2category(category2keyword):
    res = []
    for category, keywords in category2keyword.items():
        for keyword in keywords:
            if '-' in keyword: continue
            res.append([keyword, category])

    for category, keywords in category2keyword.items():
        for keyword in keywords:
            if '-' not in keyword: continue
            res.append([keyword, category])

    return res

def check3(row, keyword, category, col_name):
    keyword = keyword.strip()

    if row['Keyword Found | Work Category'] != '':
        return row['Keyword Found | Work Category']
    if keyword in row[f'{col_name} Cleaned']:
        return f"{keyword} | {category}"
    else:
        return ''

def check2(row, keyword, category, col_name):
    keyword = keyword.strip()
    key1, key2 = keyword.split(' ')

    if row['Keyword Found | Work Category'] != '':
        return row['Keyword Found | Work Category']
    if key1 in row[f'{col_name} Cleaned'] and key2 in row[f'{col_name} Cleaned']:
        return f"{keyword} | {category}"
    else:
        return ''

def check1(row, keyword, category, col_name):
    keyword = keyword.strip()
    key1, key2 = keyword.split('-')

    if row['Keyword Found | Work Category'] != '':
        return row['Keyword Found | Work Category']
    if key1 in row[f'{col_name} Cleaned'] and key2 not in row[f'{col_name} Cleaned']:
        return f"{keyword} | {category}"
    else:
        return ''


def apply_transformer(category2keyword, data):
    data['Work Name Cleaned'] = data['Work Name'].apply(clean)
    data['Asset Name Cleaned'] = data['Asset Name'].apply(clean)
    data['Work Type Cleaned'] = data['Work Type'].apply(clean)
    data['Category Cleaned'] = data['Category'].apply(clean)
    data['Sub-Category Cleaned'] = data['Sub-Category'].apply(clean)

    keyword2category = getKeywork2category(category2keyword)
    data['Keyword Found | Work Category'] = ''

    for col in ['Work Name', 'Asset Name', 'Work Type', 'Category', 'Sub-Category']:
        for keyword, category in keyword2category:
            if '-' in keyword:
                data['Keyword Found | Work Category'] = data.apply(check1, axis=1, keyword=keyword, category=category, col_name=col)
            elif ' ' in keyword:
                data['Keyword Found | Work Category'] = data.apply(check2, axis=1, keyword=keyword, category=category, col_name=col)
            else:
                data['Keyword Found | Work Category'] = data.apply(check3, axis=1, keyword=keyword, category=category, col_name=col)

    data['Attention'] = data['Keyword Found | Work Category'].apply(lambda string: string.split(' | ')[0] if len(string.split(' | ')) > 1 else '')
    data['WorkCategory'] = data['Keyword Found | Work Category'].apply(lambda string: string.split(' | ')[1] if len(string.split(' | ')) > 1 else '')

    return data

def script(data, district_name):
    # Apply transformations on the passed dataframe `data`
    result_df = apply_transformer(category2keyword, data)
    result_df.drop('Attention', inplace=True, axis=1)
    result_df.drop('Keyword Found | Work Category', inplace=True, axis=1)
    result_df.drop('Work Name Cleaned', inplace=True, axis=1)
    result_df.drop('Asset Name Cleaned', inplace=True, axis=1)
    result_df.drop('Category Cleaned', inplace=True, axis=1)
    result_df.drop('Sub-Category Cleaned', inplace=True, axis=1)

    # Save the transformed DataFrame to a CSV file
    result_df.to_csv('/home/amitesh/Documents/Project/nrega_asset_without_mis/data/ANDHRA_PRADESH/' + district_name + '.csv', index=False)
    
    # Extract rows with no WorkCategory and save them separately
    blank = result_df[(result_df['WorkCategory'].isna()) | (result_df['WorkCategory'] == "")][['Asset Name', 'Work Name', 'Work Type', 'Category', 'Sub-Category']]
    blank.to_csv('/home/amitesh/Documents/Project/nrega_asset_without_mis/data/blank_asset.csv', index=False)