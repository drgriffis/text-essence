FUTURE_PUBLICATION = 'Future publication'
MONTH_INFERRED = 'Month inferred'
DATE_UNKNOWN = 'Date unknown'
USED_AS_IS = 'Used as is'


def getSnapshotDate(publication_date, dump_date):
    '''
    Implementation rules:
    - If the publication year is greater than the dump year, use the dump date
    - If the publication month is greater than the dump month, use the dump month
    - If the publication month is not specified AND the year matches the dump year, use the dump month
    - If the publication month is not specified and the year does NOT match the dump year, skip this sample
    - Otherwise, use the publication year/month
    '''
    dump_year, dump_month, dump_day = dump_date.split('-')

    chunks = publication_date.split('-')
    year = chunks[0]

    if len(year.strip()) == 0:
        return (None, DATE_UNKNOWN)
    
    # if publication year is greater than dump year, it's a future publication,
    # so use the dump date (as the date it's available)
    if int(year) > int(dump_year):
        year, month = dump_year, dump_month
        status = FUTURE_PUBLICATION

    # if publication month is greater than dump month (in the same year), it's
    # a future publication, so use the dump date; otherwise use the publication
    # date
    elif len(chunks) > 1:
        month = chunks[1]
        if (
            (int(year) == int(dump_year))
            and (int(month) > int(dump_month))
        ):
            month = dump_month
            status = FUTURE_PUBLICATION
        else:
            status = USED_AS_IS

    # if the publication month is not specified, then:
    # (a) if the year does NOT match the dump year, we can't infer the date,
    #     so don't use this sample
    # (b) if the year DOES match the dump year, assume that the month is the
    #     dump month
    else:
        if year != dump_year:
            return (None, DATE_UNKNOWN)
        else:
            # otherwise, assume it was published the same month as the data dump
            month = dump_month
            status = MONTH_INFERRED

    return ('{0}-{1}'.format(year, month), status)
