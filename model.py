from sklearn.naive_bayes import MultinomialNB
from sklearn import linear_model
from sklearn.neural_network import MLPClassifier
import pickle

class myModel:
    def __init__(self,name,weights):
        self.name = name
        self.weights = weights

def fetchNewModel(model):
    if(model == 'MNB'):
        return myModel('MultinomialNB',MultinomialNB())
    elif(model == 'SGD'):
        return myModel('SGDClassifier',linear_model.SGDClassifier(alpha=0.0001,random_state=3,loss='squared_hinge'))
    elif(model == 'PAC'):
        return myModel('PassiveAggressiveClassifier',linear_model.PassiveAggressiveClassifier(C=0.000001,random_state=3))
    elif(model == 'MLP'):
        return myModel('MLPClassifier',MLPClassifier(activation='logistic',random_state=3))

def fetchTrainedModel(model):
    if(model == 'MNB'):
        return myModel('MultinomialNB',pickle.load(open('MNB.sav', 'rb')))
    elif(model == 'SGD'):
        return myModel('SGDClassifier',pickle.load(open('SGD.sav', 'rb')))
    elif(model == 'PAC'):
        return myModel('PassiveAggressiveClassifier',pickle.load(open('PAC.sav', 'rb')))
    elif(model=='MLP'):
        return myModel('MLPClassifier',pickle.load(open('MLP.sav', 'rb')))