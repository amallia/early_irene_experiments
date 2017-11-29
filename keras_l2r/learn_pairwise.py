import json, sys, gzip
import numpy as np
from collections import defaultdict, Counter
import keras
from keras.models import Sequential, Model
from keras.layers import Dense, Dropout, Subtract
from keras.optimizers import SGD, Adagrad, RMSprop, Adam
import keras.backend as K

def smart_open(f):
    if f.endswith('.gz'):
        return gzip.GzipFile(f)
    return open(f)

def build_model(input_dim,widths=[64,32,1],opt='adam',loss='mean_absolute_error', dropout=True, dropoutValue=0.5, activation='relu', lr=0.002):
    input_lhs = keras.layers.Input(shape=(input_dim,))
    input_rhs = keras.layers.Input(shape=(input_dim,))

    tower = Sequential()
    for idx,w in enumerate(widths):
        idim = input_dim
        if idx > 0:
            idim = widths[idx-1]
        act = activation
        if w == 1:
            act = "linear"
        tower.add(Dense(units=w, activation=act, input_dim=idim))
        if dropout:
            tower.add(Dropout(dropoutValue))

    score_lhs = tower(input_lhs)
    score_rhs = tower(input_rhs)

    comparison = keras.layers.Subtract()([score_lhs, score_rhs])

    model = Model(inputs=[input_lhs, input_rhs], outputs=comparison)
    pred_model = Model(inputs=input_lhs, outputs=score_lhs)

    opt_fn = SGD(lr=lr)
    if opt == 'adagrad':
        opt_fn = Adagrad(lr=lr)
    elif opt == 'rmsprop':
        opt_fn = RMSprop(lr=lr)
    elif opt == 'adam':
        opt_fn = Adam(lr=lr)
    model.compile(opt_fn,loss,metrics=['accuracy'])
    pred_model.compile(opt_fn, loss, metrics=['accuracy'])
    return model, pred_model

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
                for_qid['pos'] = []
                for_qid['neg'] = []
                for_qid['ids'] = []
            
            i = len(for_qid['ids'])
            for_qid['ids'].append(docid)
            truth = int(cols[0])
            for_qid['y'][i] = truth
            if truth > 0:
                for_qid['pos'].append(i)
            else:
                for_qid['neg'].append(i)
            
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

def computeAP(model, ids):
    aps = []
    for qid in ids:
        data = query_data[qid]
        if not data:
            continue
        truth = data['y']
        qn = len(truth)
        pqy = model.predict(data['x'])
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

def sample_query_pairs(train_ids, kq=10, kneg=10):
    y = []
    lhs = []
    rhs = []

    # choose a query:
    for qid in np.random.choice(train_ids, kq):
        qdata = query_data[qid]
        if not qdata:
            continue
        xs = qdata['x']
        pos = np.random.choice(qdata['pos'], 1)[0]
        neg = np.random.choice(qdata['neg'], kneg)

        # put instances in for both directions
        for ni in neg:
            y.append(1)
            lhs.append(xs[pos])
            rhs.append(xs[ni])
            y.append(-1)
            lhs.append(xs[ni])
            rhs.append(xs[pos])

    return np.array(y), np.array(lhs), np.array(rhs)

def train(cmp_model, scorer_model, train_ids, samples=1000, kq=150, kneg=50):
    for s in range(samples):
        y, lX, rX = sample_query_pairs(train_ids, kq, kneg)
        cmp_model.train_on_batch(x=[lX, rX], y=y)
        if s % 10 == 0:
            print("%d. loss: %s" % (s, cmp_model.evaluate(x=[lX,rX], y=y)))
            print("%d. mAP: %1.3f" % (s, np.mean(computeAP(scorer_model, train_ids))))

cmp_model, scorer_model = build_model(len(meta_features), activation='sigmoid')
train(cmp_model, scorer_model, train_ids)

