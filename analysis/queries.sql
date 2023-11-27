SELECT
  b.bookId,
  lb.Title,
  COUNT(*) NumRecs
FROM
  `DetectedBooks` b,
  libbook lb,
  librecs lr
WHERE
  lb.BookId = b.bookId
  and lr.bid = b.bookId
GROUP BY
  b.bookId,
  lb.Title
order by
  NumRecs DESC;