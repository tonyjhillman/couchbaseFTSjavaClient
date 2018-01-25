package main.java;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.*;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;

/**
 * For the successful running of the methods below, three indexes must exist on Couchbase
 * Server, all applied to the travel-sample bucket.
 *
 * Thee first index, "travel-sample-index-unstored", uses all the default settings.
 *
 * The second, "travel-sample-index-stored", is identical, except that it has the
 * "Store dynamic fields" box checked (in the "Advanced" settings area of the UI):
 * this allows content, potentially highlighted, to be returned.
 *
 * The third index, "travel-sample-index-hotel-description" only has the description
 * fields of hotel documents indexed.
 */
public class FtsJavaClient {

    public static void simpleTextQuery(Bucket bucket){
        String indexName = "travel-sample-index-unstored";
        MatchQuery query = SearchQuery.match("swanky");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10));

        printResult("Simple Text Query", result);
    }

    public static void simpleTextQueryOnStoredField(Bucket bucket){
        String indexName = "travel-sample-index-stored";
        MatchQuery query = SearchQuery.match("MDG").field("destinationairport");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight());

        printResult("Simple Text Query on Stored Field", result);
    }

    public static void simpleTextQueryOnNonDefaultIndex(Bucket bucket){
        String indexName = "travel-sample-index-hotel-description";
        MatchQuery query = SearchQuery.match("swanky");

        SearchQueryResult rest = bucket.query(
                new SearchQuery(indexName, query).limit(10));
    }

    public static void textQueryOnStoredFieldWithFacets(Bucket bucket){
        String indexName = "travel-sample-index-stored";
        MatchQuery query = SearchQuery.match("La Rue Saint Denis!!").field("reviews.content");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight()
                .addFacet ("Countries Referenced", SearchFacet.term("country", 5)));

        printResult("Match Query with Facet, Result by Row", result);

        System.out.println();
        System.out.println("Match Query with Facet, Result by hits:");
        System.out.println(result.hits());

        System.out.println();
        System.out.println("Match Query with Facet, Result by facet: ");
        System.out.println(result.facets());
    }

    public static void docIdQueryMethod (Bucket bucket){
        String indexName = "travel-sample-index-unstored";
        DocIdQuery query = SearchQuery.docId("hotel_26223", "hotel_28960");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query));

        printResult("DocId Query", result);
    }

    public static void unAnalyzedTermQuery(Bucket bucket, int fuzzinessLevel){
        String indexName = "travel-sample-index-stored";
        TermQuery query = SearchQuery.term("sushi").field("reviews.content").fuzziness(fuzzinessLevel);

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(50).highlight());

        printResult("Unanalyzed Term Query with Fuzziness Level of " + fuzzinessLevel + ":", result);
    }

    public static void matchPhraseQueryOnStoredField (Bucket bucket){
        String indexName = "travel-sample-index-stored";
        MatchPhraseQuery query = SearchQuery.matchPhrase("Eiffel Tower").field("description");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight());

        printResult("Match Phrase Query, using Analysis", result);
    }

    public static void unAnalyzedPhraseQuery (Bucket bucket){
        String indexName = "travel-sample-index-stored";
        PhraseQuery query = SearchQuery.phrase("dorm", "rooms").field("description");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight());

        printResult("Phrase Query, without Analysis", result);
    }

    public static void conjunctionQueryMethod (Bucket bucket){
        String indexName = "travel-sample-index-stored";
        MatchQuery firstQuery = SearchQuery.match("La Rue Saint Denis!!").field("reviews.content");
        MatchQuery secondQuery = SearchQuery.match("boutique").field("description");

        ConjunctionQuery conjunctionQuery = SearchQuery.conjuncts(firstQuery, secondQuery);

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, conjunctionQuery).limit(10).highlight());

        printResult("Conjunction Query", result);
    }

    public static void queryStringMethod (Bucket bucket){
        String indexName = "travel-sample-index-unstored";
        QueryStringQuery query = SearchQuery.queryString("description: Imperial");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10));

        printResult("Query String Query", result);
    }

    public static void wildCardQueryMethod (Bucket bucket){
        String indexName = "travel-sample-index-stored";
        WildcardQuery query = SearchQuery.wildcard("bouti*ue").field("description");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight());

        printResult("Wild Card Query", result);
    }

    public static void numericRangeQueryMethod (Bucket bucket){
        String indexName = "travel-sample-index-unstored";
        NumericRangeQuery query = SearchQuery.numericRange().min(10100).max(10200).field("id");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10));

        printResult("Numeric Range Query", result);
    }

    public static void regexpQueryMethod (Bucket bucket){
        String indexName = "travel-sample-index-stored";
        RegexpQuery query = SearchQuery.regexp("[a-z]").field("description");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query).limit(10).highlight());
    }

    private static void printResult(String label, SearchQueryResult resultObject){
        System.out.println();
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println();
        System.out.println(label);
        System.out.println();

        for (SearchQueryRow row : resultObject) {
            System.out.println(row);
        }
    }

    public static void main(String[] args) {
        // Access the cluster that is running on the local host, authenticating with
        // the username and password of any user who has the "FTS Searcher" role
        // for the "travel-sample" bucket...
        //
        Cluster cluster = CouchbaseCluster.create("localhost");
        cluster.authenticate("Administrator", "password");
        Bucket travelSample = cluster.openBucket("travel-sample");

        simpleTextQuery(travelSample);

        simpleTextQueryOnStoredField(travelSample);

        simpleTextQueryOnNonDefaultIndex(travelSample);

        textQueryOnStoredFieldWithFacets(travelSample);

        docIdQueryMethod(travelSample);

        unAnalyzedTermQuery(travelSample, 0);

        unAnalyzedTermQuery(travelSample, 2);

        matchPhraseQueryOnStoredField(travelSample);

        unAnalyzedPhraseQuery(travelSample);

        conjunctionQueryMethod(travelSample);

        queryStringMethod(travelSample);

        wildCardQueryMethod(travelSample);

        numericRangeQueryMethod(travelSample);

        regexpQueryMethod(travelSample);

        cluster.disconnect();
    }
}