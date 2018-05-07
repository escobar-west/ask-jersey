import re
import scrapy

class FredSpider(scrapy.Spider):
    name = 'askfred'

    def start_requests(self):
        start_url = 'https://askfred.net/Results/index.php'
        yield scrapy.Request(url=start_url, callback=self.parse_home)


    def parse_home(self, response):
        res_dict = {'page_url':response.url, 'tmts': []}
        # Scrape data for each tournament
        tmt_list = response.css('.evenrow, .oddrow')
        for tmt in tmt_list:
            tmt_url = tmt.css('a[href*="results.php?tournament_id"]::attr(href)').extract_first()
            # Only scrape tournament data when there are some posted results (clickable links)
            if tmt_url is not None:
                tmt_dict = {}
                tmt_dict['tmt_name'] = tmt.css('a[href*="results.php?tournament_id"]::text').extract_first()
                tmt_dict['tmt_id'] = re.search('tournament_id=(\d+)', tmt_url).group(1)
                tmt_dict['loc'] = tmt.css('td + td::text').extract_first()
                tmt_dict['date'] = tmt.css('td + td + td::text').extract_first()
                # Visit tournament page if it exists
                yield scrapy.Request(response.urljoin(tmt_url),
                                     callback=self.parse_tmt,
                                     meta={'tmt_id':tmt_dict['tmt_id']}
                                    )
                res_dict['tmts'].append(tmt_dict)
        yield res_dict
        # Go to next page if exists
        next_url = response.css('a[title="Next Page"]::attr(href)').extract_first()
        if (next_url is not None) and ('page_id=3' not in next_url):
            yield scrapy.Request(response.urljoin(next_url), callback=self.parse_home)


    def parse_tmt(self, response):
        tmt_id = response.meta['tmt_id']
        res_dict = {'tmt_url':response.url, 'tmt_id': tmt_id, 'events': []}
        # Scrape data for each event
        event_list = response.css('table.box')
        for event in event_list:
            event_url = event.css('a[href*="event_id="]::attr(href)').extract_first()
            # Only scrape event data if results were posted
            if event_url is not None:
                event_dict = {}
                event_dict['event_name'] = event.css('th[colspan="5"]::text').extract()
                event_dict['event_id'] = re.search('event_id=(\d+)', event_url).group(1)
                # Scrape data for every listed fencer
                event_dict['fencers'] = []
                fencer_list = event.css('tr.column_header ~ tr')
                for fencer in fencer_list:
                    fencer_dict = {}
                    fencer_cols = fencer.css('td')
                    fencer_dict['place'] = fencer_cols[0].css('::text').extract_first()
                    fencer_dict['fencer_name'] = fencer_cols[1].css('::text').extract_first()
                    fencer_dict['fencer_id'] = re.search('competitor_id=(\d+)', fencer_cols[1].extract()).group(1)
                    fencer_dict['club'] = fencer_cols[2].css('a.club::text').extract_first()
                    fencer_dict['rating'] = fencer_cols[3].css('::text').extract_first()
                    fencer_dict['rating_earned'] = fencer_cols[4].css('::text').extract_first()
                    event_dict['fencers'].append(fencer_dict)
                res_dict['events'].append(event_dict)
                # Go to round results if they exist
                yield scrapy.Request(response.urljoin(event_url),
                                     callback=self.parse_round,
                                     meta={'tmt_id':tmt_id, 'event_id':event_dict['event_id']}
                                    )
        yield res_dict


    def parse_round(self, response):
        # Either parse round as a pool or as a direct elimination round
        title = response.css('h2').extract_first()
        if 'Pools' in title:
            for output in self.parse_pool(response):
                yield output
        elif 'Direct Elimination' in title:
            for output in self.parse_de(response):
                yield output
        # Go to next round if it exists
        next_url = response.css('td[align="right"][valign="bottom"] a::attr(href)').extract_first()
        if next_url is not None:
            yield scrapy.Request(response.urljoin(next_url),
                                 callback=self.parse_round,
                                 meta=response.meta
                                )


    def parse_pool(self, response):
        tmt_id = response.meta['tmt_id']
        event_id = response.meta['event_id']
        res_dict = {'pool_url': response.url, 'tmt_id': tmt_id, 'event_id': event_id, 'pools': []}
        # Scrape each pool
        pool_list = response.css('table.pool_table')
        for pool in pool_list:
            pool_dict = {}
            pool_dict['pool_no'] = pool.css('th::text').extract_first()
            # Scrape pool results for each fencer
            pool_dict['fencers'] = []
            fencer_list = pool.css('tr')[2:] # Remove first two header rows
            for fencer in fencer_list:
                fencer_dict = {}
                fencer_dict['name'] = fencer.css('td.comp::text').extract_first()
                fencer_dict['pool_pos'] = fencer.css('td.comp_no b::text').extract_first()
                fencer_dict['results'] = fencer.css('td.comp_no ~ td[class=""]::text').extract()
                pool_dict['fencers'].append(fencer_dict)
            res_dict['pools'].append(pool_dict)
        yield res_dict
              

    def parse_de(self, response):
        tmt_id = response.meta['tmt_id']
        event_id = response.meta['event_id']
        res_dict = {'de_url': response.url, 'tmt_id': tmt_id, 'event_id': event_id, 'rounds': []}
        table_rows = response.css('div.debox table tr')[1:] # Remove first title row
        # Counter counts which round is being scraped (why does askfred use so many tables???)
        counter = 1
        round_results = table_rows.css(f'td:nth-child({counter}) a[onclick*=highlight]::text').extract()
        while round_results:
            res_dict['rounds'].append({'round_no': counter, 'results': round_results})
            counter += 1
            round_results = table_rows.css(f'td:nth-child({counter}) a[onclick*=highlight]::text').extract()
        yield res_dict
