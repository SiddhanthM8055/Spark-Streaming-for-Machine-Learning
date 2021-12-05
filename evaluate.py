from sklearn.metrics import classification_report

def evaluate(Y,Y_pred):
    print()
    #print(classification_report(Y, Y_pred, target_names = ['Spam', 'Ham'],output_dict=True))
    print(classification_report(Y, Y_pred, target_names = ['Spam', 'Ham']))
    print()