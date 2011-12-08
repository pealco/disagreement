from cPickle import load

#input = open('braubt_tagger.pkl', 'rb')
#TAGGER = load(input)
#input.close()            

def retag(sentence_dg):
    raw = plaintext(sentence_dg)
    tokens = nltk.word_tokenize(raw)
    return TAGGER.tag(tokens)

@composable    
def modify_tags(data):
    article, sentence_dg = data
    
    retagged_sentence = retag(sentence_dg)
    subject = find_subject(sentence_dg)
    
    subject_address = subject[0]['address']
    verb_address    = sentence_dg.root['address']
    
    try:
        verb_word, new_verb_tag = retagged_sentence[verb_address]
        subject_word, new_subject_tag = retagged_sentence[subject_address]
    except IndexError:
        return False
    
    sentence_dg.get_by_address(verb_address)['tag'] = new_verb_tag
    sentence_dg.get_by_address(subject_address)['tag'] = new_subject_tag
    
    return article, sentence_dg

@composable
def modify_subject_tags(data):
    article, sentence_dg = data
    
    retagged_sentence = retag(sentence_dg)
    subject = find_subject(sentence_dg)
    
    subject_address = subject[0]['address']
    
    try:
        subject_word, new_subject_tag = retagged_sentence[subject_address]
    except IndexError:
        return False
    
    sentence_dg.get_by_address(subject_address)['tag'] = new_subject_tag
    
    return article, sentence_dg