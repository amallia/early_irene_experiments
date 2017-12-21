import json, sys, gzip
import numpy as np
from collections import defaultdict, Counter
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import SGD, Adagrad, RMSprop
import keras.backend as K

def smart_open(f):
    if f.endswith('.gz'):
        return gzip.GzipFile(f)
    return open(f)

def build_model(input_dim,
                widths=[256, 256, 1],
                opt='sgd',
                loss='mean_absolute_error',
                dropout=True,
                dropoutValue=0.5,
                activation='relu',
                lr=0.001):
    model = Sequential()
    for idx,w in enumerate(widths):
        idim = input_dim
        if idx > 0:
            idim = widths[idx-1]
        model.add(Dense(units=w, activation=activation, input_dim=idim))
        if dropout:
            model.add(Dropout(dropoutValue))
    opt_fn = SGD(lr=lr)
    if opt == 'adagrad':
        opt_fn = Adagrad(lr=lr)
    elif opt == 'rmsprop':
        opt_fn = RMSprop(lr=lr)
    model.compile(optimizer=opt_fn,loss=loss,metrics=['accuracy'])
    return model

def before_str(string, suffix):
    if string.endswith(suffix):
        return string[:-len(suffix)]
    else:
        return string
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

    with smart_open("%s.meta.json" % before_str(ranklib_file, ".gz")) as fp:
        meta_features = dict((v-1, k) for k,v in json.loads(fp.read()).items())
    print(meta_features)

    NF = len(meta_features)
    ND = 100
    with smart_open(ranklib_file) as fp:
        for line in fp:
            cols = line.decode('utf-8').split()
            qid = after_str(cols[1], "qid:")
            docid = after_str(cols[-1], "#")

            for_qid = query_data[qid]
            if not for_qid:
                sys.stdout.write('.')
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

#load_ranklib_input('../l2rf/gov2.features.ranklib')
#train_ids = [x for x in query_data.keys() if int(x) < 800]
#vali_ids = [x for x in query_data.keys() if int(x) < 825 and int(x) >= 800]
#test_ids = [x for x in query_data.keys() if int(x) >= 825]

def computeAP(model, X, y, ids):
    py = model.predict(X)
    i = 0
    aps = []
    for qid in ids:
        data = query_data[qid]
        truth = data['y']
        qn = len(truth)
        pqy = py[i:i+qn]
        i += qn
        numRel = np.sum(truth)
        if numRel == 0:
            aps.append(0)
            continue
        recallPointCount = 0.0
        sumPrecision = 0.0
        ranked = sorted(zip(pqy, truth), reverse=True)
        for j, (pred, actual) in enumerate(ranked):
            if (actual > 0):
                rank = j+1.0
                recallPointCount += 1
                sumPrecision += (recallPointCount / rank)
        aps.append(sumPrecision / numRel)
    return aps




load_ranklib_input('../l2rf/latest/trec-car-train-10k.features.ranklib.gz')
ekeys = list(enumerate(query_data.keys()))
train_ids = [x for i, x in ekeys if i % 5 < 3]
vali_ids = [x for i, x in ekeys if i % 5 == 3]
test_ids = [x for i, x in ekeys if i % 5 == 4]

trainX = np.concatenate([query_data[x]['x'] for x in train_ids])
trainY = np.concatenate([query_data[y]['y'] for y in train_ids])
valiX = np.concatenate([query_data[x]['x'] for x in vali_ids])
valiY = np.concatenate([query_data[y]['y'] for y in vali_ids])
testX = np.concatenate([query_data[x]['x'] for x in test_ids])
testY = np.concatenate([query_data[y]['y'] for y in test_ids])

model = build_model(trainX.shape[1], activation='sigmoid')
model.fit(trainX, trainY, shuffle=True, epochs=2, batch_size=2048)

