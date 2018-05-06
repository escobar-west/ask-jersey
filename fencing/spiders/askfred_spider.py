import re
import scrapy

class FredSpider(scrapy.Spider):
    name = 'askfred'

    def start_requests(self):
        urls = [
            'https://askfred.net/Results/index.php',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        yield {'page_url':response.url}
        # Scrape tournament data on tourament list page
        tmt_list = response.css('.evenrow, .oddrow')

        for tmt in tmt_list:
            tmt_dict = {}
            tmt_dict['loc'] = tmt.css('td + td::text').extract_first()
            tmt_dict['date'] = tmt.css('td + td + td::text').extract_first()
            tmt_dict['name'] = tmt.css('a[href*="results.php?tournament_id"]::text').extract_first()
            if tmt_dict['name'] is None:
                tmt_dict['id'] = None
            else:
                url = tmt.css('a[href*="results.php?tournament_id"]::attr(href)').extract_first()
                tmt_dict['id'] = re.search('tournament_id=(\d+)', url).group(1)
                # Visit tournament page if it exists
                yield scrapy.Request(response.urljoin(url),
                                     callback=self.parse_tmt,
                                     meta={'tmt_id':tmt_dict['id']}
                                    )
            yield tmt_dict

        # Go to next page if exists
        next_url = response.css('a[title="Next Page"]::attr(href)').extract_first()
        if (next_url is not None) and ('page_id=3' not in next_url):
            yield scrapy.Request(response.urljoin(next_url), callback=self.parse)


    def parse_tmt(self, response):
        yield {'tournament_url':response.url}
        tmt_id = response.meta['tmt_id']

        # Scrape event data
        event_list = response.css('table.box')
        for event in event_list:
            event_dict = {'tmt_id':tmt_id}
            event_dict['name'] = event.css('th[colspan="5"]::text').extract()
            url = event.css('a[href*="event_id="]::attr(href)').extract_first()
            if url is not None:
                event_dict['id'] = re.search('event_id=(\d+)', url).group(1)
            
            # Scrape event data for every listed fencer
            event_dict['fencers'] = []
            fencer_list = event.css('tr.column_header ~ tr')
            for fencer in fencer_list:
                fencer_dict = {}
                fencer_cols = fencer.css('td')
                fencer_dict['place'] = fencer_cols[0].css('::text').extract_first()
                fencer_dict['name'] = fencer_cols[1].css('::text').extract_first()
                fencer_dict['id'] = re.search('competitor_id=(\d+)', fencer_cols[1].extract()).group(1)
                fencer_dict['club'] = fencer_cols[2].css('a.club::text').extract_first()
                fencer_dict['rating'] = fencer_cols[3].css('::text').extract_first()
                fencer_dict['rating_earned'] = fencer_cols[4].css('::text').extract_first()
                event_dict['fencers'].append(fencer_dict)
            yield event_dict

            # Go to round results if they exist
            if url is not None:
                yield scrapy.Request(response.urljoin(url),
                                     callback=self.parse_round,
                                     meta={'event_id':event_dict['id']}
                                    )


    def parse_round(self, response):
        event_id = response.meta['event_id']
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
                                 meta={'event_id':event_id}
                                )


    def parse_pool(self, response):
        event_id = response.meta['event_id']
        yield {'event_id': event_id, 'pool_url': response.url}
        pool_list = response.css('table.pool_table')
        for pool in pool_list:
            pool_dict = {'event_id': event_id}
            pool_dict['pool'] = pool.css('th::text').extract_first()

            pool_dict['fencers'] = []
            # The first two rows are for the title and column headers, we don't need them
            fencer_list = pool.css('tr')[2:]
            for fencer in fencer_list:
                fencer_dict = {}
                fencer_dict['name'] = fencer.css('td.comp::text').extract_first()
                fencer_dict['pool_no'] = fencer.css('td.comp_no b::text').extract_first()
                fencer_dict['results'] = fencer.css('td.comp_no ~ td[class=""]::text').extract()
                pool_dict['fencers'].append(fencer_dict)
            yield pool_dict
              

    def parse_de(self, response):
        event_id = response.meta['event_id']
        yield {'event_id': event_id, 'de_url': response.url}
