def value_cleaning(value):
    # Replace punctuation characters
    step1 = value.replace(".","").replace(",","").replace(";","").replace(":","").replace("?","").replace("!","").replace("'","").replace("`","").replace("…","").replace("—","")
    # Mathematical Symbols
    step2 = step1.replace("=", "").replace("+","").replace("-","").replace("<","").replace(">","").replace("%","").replace("∞","").replace("*","").replace("/","").replace("|","")
    # Brackets & Currency Symbols
    step3 = step2.replace("{", "").replace("}","").replace("[","").replace("]","").replace("(","").replace(")","").replace("$","").replace("₹","")
    # Miscellaneous Symbols
    step4 = step3.replace("©", "").replace("®","").replace("™","").replace("✓","").replace("∆","")
    # Keyboard Special Characters
    step5 = step4.replace("@", "").replace("#","").replace("^","").replace("&","").replace("~","")
    # Whitespace Characters
    step6 = step5.replace(" ", "").replace("\n","").replace("\t","").strip()
    return step6

# ====== To keep space ' '
def value_cleaning_s(value):
    # Replace punctuation characters
    step1 = value.replace(".","").replace(",","").replace(";","").replace(":","").replace("?","").replace("!","").replace("'","").replace("`","").replace("…","").replace("—","")
    # Mathematical Symbols
    step2 = step1.replace("=", "").replace("+","").replace("-","").replace("<","").replace(">","").replace("%","").replace("∞","").replace("*","").replace("/","").replace("|","")
    # Brackets & Currency Symbols
    step3 = step2.replace("{", "").replace("}","").replace("[","").replace("]","").replace("(","").replace(")","").replace("$","").replace("₹","")
    # Miscellaneous Symbols
    step4 = step3.replace("©", "").replace("®","").replace("™","").replace("✓","").replace("∆","")
    # Keyboard Special Characters
    step5 = step4.replace("@", "").replace("#","").replace("^","").replace("&","").replace("~","")
    # Whitespace Characters
    step6 = step5.replace("\n","").replace("\t","").strip()
    return step6

# ====== To keep '-'
def value_cleaning_m(value):
    # Replace punctuation characters
    step1 = value.replace(".","").replace(",","").replace(";","").replace(":","").replace("?","").replace("!","").replace("'","").replace("`","").replace("…","").replace("—","")
    # Mathematical Symbols
    step2 = step1.replace("=", "").replace("+","").replace("<","").replace(">","").replace("%","").replace("∞","").replace("*","").replace("/","").replace("|","")
    # Brackets & Currency Symbols
    step3 = step2.replace("{", "").replace("}","").replace("[","").replace("]","").replace("(","").replace(")","").replace("$","").replace("₹","")
    # Miscellaneous Symbols
    step4 = step3.replace("©", "").replace("®","").replace("™","").replace("✓","").replace("∆","")
    # Keyboard Special Characters
    step5 = step4.replace("@", "").replace("#","").replace("^","").replace("&","").replace("~","")
    # Whitespace Characters
    step6 = step5.replace(" ", "").replace("\n","").replace("\t","").strip()
    return step6

# ====== To keep ' ' and '-' and ':'
def value_cleaning_scm(value):
    # Replace punctuation characters
    step1 = value.replace(".","").replace(",","").replace(";","").replace("?","").replace("!","").replace("'","").replace("`","").replace("…","").replace("—","")
    # Mathematical Symbols
    step2 = step1.replace("=", "").replace("+","").replace("<","").replace(">","").replace("%","").replace("∞","").replace("*","").replace("/","").replace("|","")
    # Brackets & Currency Symbols
    step3 = step2.replace("{", "").replace("}","").replace("[","").replace("]","").replace("(","").replace(")","").replace("$","").replace("₹","")
    # Miscellaneous Symbols
    step4 = step3.replace("©", "").replace("®","").replace("™","").replace("✓","").replace("∆","")
    # Keyboard Special Characters
    step5 = step4.replace("@", "").replace("#","").replace("^","").replace("&","").replace("~","")
    # Whitespace Characters
    step6 = step5.replace("\n","").replace("\t","").strip()
    return step6

# ====== To keep '.'
def value_cleaning_d(value):
    # Replace punctuation characters
    step1 = value.replace(",","").replace(";","").replace(":","").replace("?","").replace("!","").replace("'","").replace("`","").replace("…","").replace("—","")
    # Mathematical Symbols
    step2 = step1.replace("=", "").replace("+","").replace("-","").replace("<","").replace(">","").replace("%","").replace("∞","").replace("*","").replace("/","").replace("|","")
    # Brackets & Currency Symbols
    step3 = step2.replace("{", "").replace("}","").replace("[","").replace("]","").replace("(","").replace(")","").replace("$","").replace("₹","")
    # Miscellaneous Symbols
    step4 = step3.replace("©", "").replace("®","").replace("™","").replace("✓","").replace("∆","")
    # Keyboard Special Characters
    step5 = step4.replace("@", "").replace("#","").replace("^","").replace("&","").replace("~","")
    # Whitespace Characters
    step6 = step5.replace(" ", "").replace("\n","").replace("\t","").strip()
    return step6

# ====== For word's cleaning
def word_cleaning(value):
    step1 = value.replace('bbadb','').replace('x1F60A','').replace('ext123','').replace('x160A','').replace('bbab','')
    step2 = step1.replace('x1F0A','').replace('x1F6A','').replace('badb','').replace('1F60A','')
    step3 = step2.replace('xF60A','').replace('bbad','').replace('x1F60','').replace('bbdb','').strip()
    return step3

# ====== For letter's cleaning
def letter_cleaning(value):
    step1 = value.replace('X','').replace('Y','').replace('Z','').strip()
    return step1

# ====== Fix restaurant_id in order table
def fix_ids(value):
    match value:
        case 'R01':
            return 'R001'
        case 'R02':
            return 'R002'
        case 'R03':
            return 'R003'
        case 'R04':
            return 'R004'
        case 'R05':
            return 'R005'
        case 'R06':
            return 'R006'
        case 'R07':
            return 'R007'
        case 'R08':
            return 'R008'
        case 'R09':
            return 'R009'
        case 'R10':
            return 'R010'
    return value