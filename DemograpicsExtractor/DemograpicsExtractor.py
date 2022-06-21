import re

class DemograpicsExtractor():
    
    _short_self_term = "(i am|i\'m|me being)"
    _self_term = "((i|((i\'m|i am)( a)?))|me|my|my selfe)"
    _optional_word = "([!-~]* )?"
    _man_term = "(guy|man|boy|boyfriend|husband)"
    _woman_term = "(woman|girl|gal|girlfriend|wife)"
    _male_term = f"(a {_optional_word}{_man_term}|(a {_optional_word})?male)"
    _female_term = f"(a {_optional_word}{_woman_term}|(a {_optional_word})?female)"
    _open_bracket = "(\(|\[)"
    _close_bracket = "(\)|\])"
    _begin_term = "([^\w]|^)"
    _not_hypothetical_begin_term = "(^|((?<!when)(?<!if)(?<!until)[^\w]))"
    _end_term = "([^\w]|$)"
    _end_term_2 = "(((?=[^\w])(?=([^\-]|\- )))|$)"

    _m1 = f" ?{_open_bracket}(\d\dm|m\d\d|\d\d m|m \d\d){_close_bracket}"
    _m2 = f" (\d\d|\d) ?{_open_bracket}m{_close_bracket}"
    _m3 = f" ?{_open_bracket}m{_close_bracket} ?(\d\d|\d){_end_term}"
    _m4 = f" (\d\dm|m\d\d|\d\d m|m \d\d){_end_term}"

    _f1 = f" ?{_open_bracket}(\d\df|f\d\d|\d\d f|f \d\d){_close_bracket}"
    _f2 = f" (\d\d|\d) ?{_open_bracket}f{_close_bracket}"
    _f3 = f" ?{_open_bracket}f{_close_bracket} ?(\d\d|\d){_end_term}"
    _f4 = f" (\d\df|f\d\d|\d\d f|f \d\d){_end_term}"
    
    def determine_gender(self, text, remove_quotes=True):
        if remove_quotes:
            text = self.strip_quotes(text)
        if bool(re.search(f"{self._begin_term}{self._short_self_term} {self._male_term}(((?=[^\w])(?=([^\-]|\- )))|$)",text)) or \
        bool(re.search(f"{self._begin_term}{self._self_term}({self._m1}|{self._m2}|{self._m3}|{self._m4}){self._end_term}",text)):
            return 'M'

        if bool(re.search(f"{self._begin_term}{self._short_self_term} {self._female_term}(((?=[^\w])(?=([^\-]|\- )))|$)",text)) or \
        bool(re.search(f"{self._begin_term}{self._self_term}({self._f1}|{self._f2}|{self._f3}|{self._f4}){self._end_term}",text)):
            return 'F'

    def determine_age(self, text, remove_quotes=True):
        if remove_quotes:
            text = self.strip_quotes(text)
        match_o = re.search(f'{self._not_hypothetical_begin_term}(i am|i\'m) (\d\d+) (years|yrs|yr)? old{self._end_term}', text)
        if not match_o:
            match_o = re.search(f"{self._begin_term}{self._self_term} ?({self._m1}|{self._m2}|{self._m3}|{self._m4}|{self._f1}|{self._f2}|{self._f3}|{self._f4}){self._end_term}",text)
        if match_o:
            demographics = text[match_o.start():match_o.end()]
            return re.findall(r'\d+',demographics)[0]
  

    def return_gender_match(self,text):
        match = re.search(f"{self._begin_term}{self._short_self_term} {self._male_term}{self._end_term}",text)
        if match:
            return match
        
        match = re.search(f"{self._begin_term}{self._self_term}({self._m1}|{self._m2}|{self._m3}|{self._m4}){self._end_term}",text)
        if match:
            return match
        
        match = re.search(f"{self._begin_term}{self._short_self_term} {self._female_term}{self._end_term}",text)
        if match:
            return match
        
        match = re.search(f"{self._begin_term}{self._self_term}({self._f1}|{self._f2}|{self._f3}|{self._f4}){self._end_term}",text)
        if match:
            return match


    def return_age_match (self,text):
        match_o = re.search(f'{self._not_hypothetical_begin_term}(i am|i\'m) (\d+) (years|yrs|yr)? old{self._end_term}', text)
        if match_o:
            return match_o

        match_o = re.search(f"{self._begin_term}{self._self_term} ?({self._m1}|{self._m2}|{self._m3}|{self._m4}|{self._f1}|{self._f2}|{self._f3}|{self._f4}){self._end_term}",text)
        if match_o:
            return match_o
        
    def strip_quotes(self, text):
        matches = re.findall('("[^"]+"|“[^"]+”)',text)
        for m in matches:
            text = text.replace(m,'')
        text = text.replace('  ', ' ')
        return text
