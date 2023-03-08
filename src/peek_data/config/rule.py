from .parser import PeekingConfig
from ..core.rule import ABCRule, RuleGroup, MultipleRulesViolation


class DataStreamMappingKeywords(ABCRule):
    def check(self, config: PeekingConfig) -> bool:
        for itf_mapping in config['interface-mapping']:
            if itf_mapping.get('from') is None or \
                itf_mapping.get('to') is None:
                return False
        return True                
    
    @property
    def description(self) -> str:
        return "Keywords `from` and `to` are mandatory in the interface mapping item."


DataStreamRuleGroup = RuleGroup([DataStreamMappingKeywords()])
