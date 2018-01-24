package main.java;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.*;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;

public class FtsJavaClient
{
    private static MatchQuery matchQuery;

    private static MatchQuery secondMatchQuery;

    private static ConjunctionQuery conjunctionQuery;

    private static DisjunctionQuery disjunctionQuery;

    private static MatchPhraseQuery matchPhraseQuery;

    private static TermQuery termQuery;

    private static PhraseQuery phraseQuery;

    private static DocIdQuery docIdQuery;

    private static QueryStringQuery queryStringQuery;

    private static WildcardQuery wildcardQuery;

    private static NumericRangeQuery numericRangeQuery;

    private static SearchQueryResult searchQueryResult;

    private static RegexpQuery regexpQuery;

    private static int counter;

    public static void simpleTextQuery (Bucket bucket, String textForQuery,
                                        String indexName, int maxResultsDisplayed)
    {
        matchQuery = SearchQuery.match(textForQuery);

        searchQueryResult = bucket.query(
                new SearchQuery (indexName, matchQuery).limit(maxResultsDisplayed));

        printResultAndDividers(searchQueryResult);
    }

    public static void simpleTextQueryOnStoredField (Bucket bucket, String textForQuery,
                                                     String indexName, String fieldName,
                                                     int maxResultsDisplayed)
    {
        matchQuery = SearchQuery.match(textForQuery).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery (indexName, matchQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void textQueryOnStoredFieldWithFacets (Bucket bucket, String textForQuery,
                                                         String indexName, String fieldName,
                                                         int maxResultsDisplayed, String facetName,
                                                         String facetField, int facetLimit)
    {
        matchQuery = SearchQuery.match(textForQuery).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery (indexName, matchQuery).limit(maxResultsDisplayed).highlight()
                        .addFacet (facetName, SearchFacet.term(facetField, facetLimit)));

        System.out.println("By row:");
        for (SearchQueryRow row : searchQueryResult)
        {
            System.out.println(row);
        }
        System.out.println();

        System.out.println("By hits:");
        System.out.println(searchQueryResult.hits());
        System.out.println();

        System.out.println("By facet: ");
        System.out.println(searchQueryResult.facets());
        System.out.println();
    }

    public static void matchPhraseQueryOnStoredField (Bucket bucket, String phraseForQuery,
                                                      String indexName, String fieldName,
                                                      int maxResultsDisplayed)
    {
        matchPhraseQuery = SearchQuery.matchPhrase(phraseForQuery).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, matchPhraseQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void unAnalyzedTermQuery (Bucket bucket, String searchTerm, String indexName,
                                      String fieldName, int maxResultsDisplayed,
                                      int fuzzinessLevel)
    {
        termQuery = SearchQuery.term(searchTerm).field(fieldName).fuzziness(fuzzinessLevel);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, termQuery).limit(maxResultsDisplayed).highlight());

        counter = 0;

        for (SearchQueryRow row : searchQueryResult)
        {
            System.out.println(row);
            counter++;
        }

        System.out.println("Number of rows returned for " + searchTerm + " with fuzziness of "
                + fuzzinessLevel + " is " + counter);

        printDividers();
    }

    public static void unAnalyzedPhraseQuery (Bucket bucket, String firstTerm, String secondTerm,
                                        String indexName, String fieldName,
                                        int maxResultsDisplayed)
    {
        phraseQuery = SearchQuery.phrase(firstTerm, secondTerm).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery (indexName, phraseQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void conjunctionQueryMethod (Bucket bucket, String firstTerm, String firstField,
                                          String secondTerm, String secondField,
                                          String indexName, int maxResultsDisplayed)
    {
        matchQuery = SearchQuery.match(firstTerm).field(firstField);

        secondMatchQuery = SearchQuery.match(secondTerm).field(secondField);

        conjunctionQuery = SearchQuery.conjuncts(matchQuery, secondMatchQuery);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, conjunctionQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void disjunctionQueryMethod (Bucket bucket, String firstTerm, String firstField,
                                               String secondTerm, String secondField,
                                               String indexName, int maxResultsDisplayed,
                                               int disjunctionSource)
    {
        matchQuery = SearchQuery.match(firstTerm).field(firstField);

        secondMatchQuery = SearchQuery.match(secondTerm).field(secondField);

        disjunctionQuery = SearchQuery.disjuncts(matchQuery, secondMatchQuery).min(disjunctionSource);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, disjunctionQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void docIdQueryMethod (Bucket bucket, String firstDocId, String secondDocId,
                                    String indexName)
    {
        docIdQuery = SearchQuery.docId(firstDocId, secondDocId);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, docIdQuery));

        printResultAndDividers(searchQueryResult);
    }

    public static void queryStringQueryMethod (Bucket bucket, String queryString, String indexName,
                                          int maxResultsDisplayed)
    {
        queryStringQuery = SearchQuery.queryString(queryString);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, queryStringQuery).limit(maxResultsDisplayed));

        printResultAndDividers(searchQueryResult);
    }

    public static void wildCardQueryMethod (Bucket bucket, String wildcardString, String indexName,
                                             String fieldName, int maxResultsDisplayed)
    {
        wildcardQuery = SearchQuery.wildcard(wildcardString).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, wildcardQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void numericRangeQueryMethod (Bucket bucket, int minimum, int maximum, String fieldName,
                                                 String indexName, int maxResultsDisplayed)
    {
        numericRangeQuery = SearchQuery.numericRange().min(minimum).max(maximum).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, numericRangeQuery).limit(maxResultsDisplayed));

        printResultAndDividers(searchQueryResult);
    }

    public static void regexpQueryMethod (Bucket bucket, String regExp, String fieldName,
                                           String indexName, int maxResultsDisplayed)
    {
        regexpQuery = SearchQuery.regexp(regExp).field(fieldName);

        searchQueryResult = bucket.query(
                new SearchQuery(indexName, regexpQuery).limit(maxResultsDisplayed).highlight());

        printResultAndDividers(searchQueryResult);
    }

    public static void printQueryNumber(int queryNumber)
    {
        System.out.println("Query " + Integer.toString(queryNumber) +": ");
        System.out.println();
    }

    public static void printDividers ()
    {
        System.out.println();
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println();
    }

    private static void printResultAndDividers(SearchQueryResult resultObject)
    {
        for (SearchQueryRow row : resultObject)
        {
            System.out.println(row);
        }

        printDividers();
    }

    public static void main(String[] args)
    {
        // Access the cluster that is running on the local host, authenticating with
        // the username and password of any user who has the "FTS Searcher" role
        // for the "travel-sample" bucket...
        //
        Cluster cluster = CouchbaseCluster.create("localhost");

        System.out.print("Authenticating as administrator." + "\n");
        cluster.authenticate("Administrator", "password");

        // Open the travel-sample bucket.
        //
        Bucket travelSample = cluster.openBucket("travel-sample");

        System.out.println();

        // For the successful running of the routines below, three indexes must exist on Couchbase
        // Server, all applied to the travel-sample bucket. Each of the index-definition files is
        // included in this repository:
        //
        // The first index, "travel-sample-index-unstored", uses all
        // the default settings.
        //
        // The second, "travel-sample-index-stored", is identical, except that
        // it has the "Store dynamic fields" box checked (in the "Advanced" settings area of the UI):
        // this allows content, potentially highlighted, to be returned.
        //
        // The third index, "travel-sample-index-hotel-description" only has the description fields
        // of hotel documents indexed.
        //

        // A Match Query analyzes the input text and uses the result as the query-input. No field
        // is specified.
        //
        printQueryNumber(1);

        simpleTextQuery(travelSample, "route",
                "travel-sample-index-unstored", 10);

        // Another Match Query, analyzes the input text, and looks for a match on a specific field.
        //
        printQueryNumber(2);

        simpleTextQueryOnStoredField(travelSample, "MDG",
                "travel-sample-index-stored", "destinationairport", 10);

        // Again a Match Query, applies a facet to the results.
        //
        printQueryNumber(3);

        textQueryOnStoredFieldWithFacets(travelSample, "La Rue Saint Denis!!",
                "travel-sample-index-stored", "reviews.content", 10,
                "Countries Referenced", "country", 5);

        // On a docID.
        //
        printQueryNumber(4);

        docIdQueryMethod(travelSample, "hotel_26223", "hotel_28960",
                "travel-sample-index-unstored");

        // On a term.
        //
        printQueryNumber(5);

        unAnalyzedTermQuery(travelSample, "sushi",
                "travel-sample-index-stored", "reviews.content", 50,
                0);

        // Same as 5, but with higher fuzziness.
        //
        printQueryNumber(6);

        unAnalyzedTermQuery(travelSample, "sushi",
                "travel-sample-index-stored", "reviews.content", 50,
                2);

        // Match on a phrase, using analysis.
        //
        //
        printQueryNumber(7);

        matchPhraseQueryOnStoredField(travelSample, "Eiffel Tower",
                "travel-sample-index-stored", "description", 10);

        // Phrase query, without analysis
        //
        printQueryNumber(8);

        unAnalyzedPhraseQuery(travelSample, "dorm", "rooms",
                "travel-sample-index-stored", "description", 10);

        // Match Query, specifying an index that contains the description field of the hotel-type
        // documents only.
        //
        printQueryNumber(9);

        simpleTextQuery(travelSample, "swanky",
                "travel-sample-index-hotel-description", 10);

        // Conjunction-set of different match queries.
        //
        printQueryNumber(10);

        conjunctionQueryMethod(travelSample, "La Rue Saint Denis!!", "reviews.content",
                "boutique", "description", "travel-sample-index-stored",
                10);

        // Disjunction-set of different match queries.
        //
        printQueryNumber(100);

        disjunctionQueryMethod(travelSample,"La Rue Saint Denis!!", "reviews.content",
                "boutique", "description", "travel-sample-index-stored",
                10, 0);

        // Query String Query.
        //
        printQueryNumber(11);

        queryStringQueryMethod(travelSample, "description: Imperial",
                "travel-sample-index-unstored", 10);

        // Wildcard Query. Note the specification of the word "boutique", using
        // a wildcard-character.
        //
        printQueryNumber(12);

        wildCardQueryMethod(travelSample, "bouti*ue", "travel-sample-index-stored",
                "description", 10);

        // Numeric Range Query. Returns all documents whose id is between the stated minimum
        // and maximum values.
        //
        printQueryNumber(13);

        numericRangeQueryMethod (travelSample, 10100, 10200, "id",
                "travel-sample-index-unstored", 10);

        // Regexp Query.
        //
        printQueryNumber(14);

        regexpQueryMethod(travelSample, "[a-z]", "description",
                "travel-sample-index-stored", 10);

        // Disconnect from the cluster.
        //
        System.out.println("Disconnecting.");
        cluster.disconnect();
    }
}