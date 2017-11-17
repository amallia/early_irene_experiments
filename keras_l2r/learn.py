import json, sys, gzip
import numpy as np
from collections import defaultdict, Counter
from keras.models import Sequential
from keras.layers import Dense

def smart_open(f):
    if f.endswith('.gz'):
        return gzip.open(f)
    return open(f)

def build_model(input_dim,widths=[256,256,1],opt='sgd',loss='mean_absolute_error'):
    model = Sequential()
    for idx,w in enumerate(widths):
        idim = input_dim
        if idx > 0:
            idim = widths[idx-1]
        model.add(Dense(units=w, activation='relu', input_dim=idim))
    model.compile(optimizer=opt,loss=loss,metrics=['accuracy'])
    return model

def after_str(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    else:
        ERROR

meta_features = {}
query_data = defaultdict(dict)

def load_ranklib_input(ranklib_file):
    global query_data
    global meta_features

    with open("%s.meta.json" % ranklib_file) as fp:
        meta_features = dict((v-1, k) for k,v in json.loads(fp.read()).items())
    print(meta_features)

    NF = len(meta_features)
    ND = 1000
    with open(ranklib_file) as fp:
        for line in fp:
            cols = line.split()
            qid = after_str(cols[1], "qid:")
            docid = after_str(cols[-1], "#")

            for_qid = query_data[qid]
            if not for_qid:
                sys.stdout.write(qid+' ')
                sys.stdout.flush()
                for_qid['x'] = np.zeros((ND,NF))
                for_qid['y'] = np.zeros(ND)
                for_qid['ids'] = []
            
            i = len(for_qid['ids'])
            for_qid['ids'].append(docid)
            for_qid['y'][i] = int(cols[0])
            
            qiX = for_qid['x'][i]
            for col in cols[2:-1]:
                kv = col.split(":")
                fid = int(kv[0])-1
                fval = float(kv[1])
                qiX[fid] = fval
    print("\nDone loading %s." % (ranklib_file))

load_ranklib_input('../l2rf/gov2.features.ranklib')
train_ids = [x for x in query_data.keys() if int(x) < 800]
vali_ids = [x for x in query_data.keys() if int(x) < 825 and int(x) >= 800]
test_ids = [x for x in query_data.keys() if int(x) >= 825]
trainX = np.concatenate([query_data[x]['x'] for x in train_ids])
trainY = np.concatenate([query_data[y]['y'] for y in train_ids])
model = build_model(trainX.shape[1])
model.fit(trainX, trainY, shuffle=True, epochs=5)

