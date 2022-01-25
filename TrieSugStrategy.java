package com.kingdomai.ais.sug.trie;

import com.kingdomai.ais.sug.factory.SuggestionFactory;
import com.kingdomai.ais.sug.model.SugQuery;
import com.kingdomai.ais.sug.model.SuggestionGroup;
import com.kingdomai.ais.sug.model.strategy.BaseSuggestion;
import com.kingdomai.ais.sug.model.strategy.SingleSugStrategy;
import com.kingdomai.ais.sug.service.MappingService;
import com.kingdomai.ais.sug.trie.service.SearchSug;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

//@Profile("!devyy")
@Slf4j
@Component
public class TrieSugStrategy extends SingleSugStrategy {
    public static final String NAME = "DoubleArrayTrie";

    @Autowired
    SuggestionFactory suggestionFactory;

    @Override
    public List<SuggestionGroup> suggest(SugQuery query) {

        boolean limit = query.getKey().length() < 2;
        // 待返回数据
        final String queryKey = query.getKey();
        List<SuggestionGroup> result = new ArrayList<>();
        query.getChannels().forEach(channel -> {
            final String[] types = channel.getSugMappings().stream().map(MappingService.Mapping::getType).toArray(String[]::new);
            List<BaseSuggestion> list = query(limit, queryKey, channel.getChannelCode(), types);
            result.add(new SuggestionGroup(channel.getChannelCode(), (long) list.size(), list));
        });

        return result;
    }

    private List<BaseSuggestion> query(boolean limit, String queryKey, String channelCode, String[] types) {
        long start = System.currentTimeMillis();
        Map<String, List<Integer>> idsMap = SearchSug.searcher(queryKey, limit, types);
        long searchSpan = System.currentTimeMillis() - start;

        if (MapUtils.isEmpty(idsMap)) {
            log.info("DoubleArrayTrie: Channel code {} ; Data size 0; Search time {}; get map time 0;"
                    , channelCode, searchSpan);
            return Collections.emptyList();
        }

        List<BaseSuggestion> result = new ArrayList<>();

        long startScore = System.currentTimeMillis();
        idsMap.entrySet().forEach(entry -> handleIdsMap(entry, result, channelCode));//todo paging?
        long getMapSpan = System.currentTimeMillis() - startScore;
        log.info("DoubleArrayTrie: Channel code {} ; Data size {}; Search time {}; get map time {};"
                , channelCode, result.size(), searchSpan, getMapSpan);
        return result;
    }

    private void handleIdsMap(Map.Entry<String, List<Integer>> entry, List<BaseSuggestion> result, String channelCode) {

        String type = entry.getKey();
        List<Integer> value = entry.getValue();

        final List<?> dic = SearchSug.getDic(type);
        for (Integer id : value) {
            final Object o = dic.get(id);
            if (o == null) {
                log.error("No data for [id {} type {}] ", id, type);
                continue;
            }

            final BaseSuggestion suggestion = suggestionFactory.createSuggestion(o, type, NAME);
            suggestion.setChannel(channelCode);
            result.add(suggestion);
        }

    }

    @Override
    public String getName() {
        return NAME;
    }
}