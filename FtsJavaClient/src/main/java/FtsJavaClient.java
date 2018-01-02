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
    public static void main(String[] args)
    {
        // Access the cluster that is running on the local host, authenticating with
        // the username and password of the Full Administrator. This
        // provides all privileges.
        //
        Cluster cluster = CouchbaseCluster.create("localhost");

        System.out.print("Authenticating as administrator." + "\n");
        cluster.authenticate("Administrator", "password");

        // Open the travel-sample bucket.
        //
        Bucket travelSample = cluster.openBucket("travel-sample");

        System.out.println('\n');

        // FTS Querying Begins Here
        //
        // For the successful running of the routines below, two indexes must exist on Couchbase Server. The
        // first, "travel-sample-index-unstored", uses all the default settings, but adds an analyzer,
        // named singleAnalyzer, which uses the "single" tokenizer. The second, "travel-sample-index-stored",
        // was defined with the "Store dynamic fields" box checked, and with the addition of an analyzer named
        // letterAnalyzer, which uses the "letter" tokenizer.
        //
        // A Match Query analyzes the input text and uses the result as the query-input.
        //
        // Limit the result-set to 10.
        //
        MatchQuery myMatchQuery01 = SearchQuery.match("route");

        SearchQueryResult mySearchQueryResult01 = travelSample.query(
                new SearchQuery("travel-sample-index-unstored", myMatchQuery01).limit(10));

        System.out.println("Query 1 (MatchQuery on \"route\" in travel-sample-index-unstored): ");
        System.out.println("\n");
        System.out.println("Note: The specified index was defined with dynamic fields \"unstored\", and so the output to this query does not matching content: it only shows doc IDs.");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult01)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 2. Again, a Match Query.
        //
        // Look for a match on the string "MDG" in
        // documents' "destinationairport" field. Limit the result-set to 10.
        //
        // FIX: The matching content is not displayed unless I also specify "highlight()". Is this intentional?
        //
        MatchQuery myMatchQuery02 = SearchQuery.match("MDG").field("destinationairport");

        SearchQueryResult mySearchQueryResult02 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myMatchQuery02).limit(10).highlight());

        System.out.println("Query 2 (MatchQuery on \"MDG\" in \"destinationairport\" fields of travel-sample-index-stored): ");
        System.out.println("\n");
        System.out.println("Note: The specified index was defined with dynamic fields \"stored\", and so the output DOES show matching content as well as the doc IDs. Note also");
        System.out.println("that highlighting has been specified in the query, so that matched elements in the content are highlighted with <mark> tags.");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult02)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 3. Again, a Match Query.
        //
        // Look for a match on the string
        // "La Rue Saint Denis!!" in documents' "reviews.content" field - note how "reviews.content" addresses
        // a field nested within a sub-object in the JSON document. Limit the result-set to 10.
        //
        MatchQuery myMatchQuery03 = SearchQuery.match("La Rue Saint Denis!!").field("reviews.content").analyzer("standard");

        SearchQueryResult mySearchQueryResult03 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myMatchQuery03)
                        .limit(10).highlight()

                        // Additionally, this query also demonstrates how to apply a "facet", whereby the incidence of a particular
                        // field across documents in the bucket (eg, "country", "city", "street") is calculated and displayed, with
                        // the incidence of each field-value displayed.
                        //
                        .addFacet("Countries Referenced", SearchFacet.term("country", 5))
                        .addFacet("Cities Referenced", SearchFacet.term("city", 5))
                );

        System.out.println("Query 3 (MatchQuery on \"La Rue Saint Denis!!\" in \"reviews.content\" fields of travel-sample-index-stored, with standard analyzer): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult03)
        {
            System.out.println(row);
        }

        // An alternative way of displaying results.
        //
        System.out.println('\n');
        System.out.println("Here, the same Query 3 results, but delivered as a list by the hits() method on the SearchQueryResult object: ");
        System.out.println(mySearchQueryResult03.hits());

        System.out.println('\n');
        System.out.println("Here, again from the Query 3 results, the 3 most frequently referenced countries, then cities, accessed via a \"facet\":  ");
        System.out.println(mySearchQueryResult03.facets());


        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 4. On a docID.
        //
        DocIdQuery myDocIdQuery04 = SearchQuery.docId("hotel_26223", "hotel_28960");

        SearchQueryResult mySearchQueryResult04 = travelSample.query(
                new SearchQuery( "travel-sample-index-unstored", myDocIdQuery04));

        System.out.println("Query 4: Result of a search on the docIDs \"hotel_26223\" and \"hotel_28960\" for travel-sample-index-unstored: ");

        for (SearchQueryRow row: mySearchQueryResult04)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 5. On a term. Note that the previously-matched "La Rue Saint Denis!!" would, if specified
        // as a term, return nothing.
        //
        // Note that fuzziness is specified as 0. See Query 6, below, for a different fuzziness specification.
        //
        TermQuery myTermIdQuery05 = SearchQuery.term("sushi").field("reviews.content")
                .fuzziness(0);

        SearchQueryResult mySearchQueryResult05 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myTermIdQuery05).limit(100).highlight());

        System.out.println("Query 5: (term-matches of \"sushi\" in \"reviews.content\" fields of travel-sample-index-stored, with fuzziness of 0 producing exact matches): ");

        int x = 0;
        for (SearchQueryRow row: mySearchQueryResult05)
        {
            System.out.println(row);
            x++;
        }

        System.out.println("Number of rows returned for \"sushi\" with fuzziness of 0 is " + x);

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 6. On the same term as used for Query 5.
        //
        // Note that fuzziness is now specified as 2.
        //
        TermQuery myTermIdQuery06 = SearchQuery.term("sushi").field("reviews.content")
                .fuzziness(2);

        SearchQueryResult mySearchQueryResult06 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myTermIdQuery06).limit(100).highlight());

        System.out.println("Query 6: (term-matches of \"sushi\" in \"reviews.content\" fields of travel-sample-index-stored, with fuzziness of 2 producing approximate matches): ");

        x = 0;
        for (SearchQueryRow row: mySearchQueryResult06)
        {
            System.out.println(row);
            x++;
        }

        System.out.println("Number of rows returned for \"sushi\" with fuzziness of 2 is " + x);

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 7. Match on a phrase.
        //
        // FIX: This won't work with more than a certain number of
        // characters, and won't work with uppercase specified. Both produce zero return.
        //
        MatchPhraseQuery myMatchPhraseQuery07 = SearchQuery.matchPhrase("the few rooms with")
                .field("description")
                .analyzer("standard");

        SearchQueryResult mySearchQueryResult07 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myMatchPhraseQuery07).limit(100).highlight());

        System.out.println("Query 7: (MatchPhrase query on \"the few rooms with\" in \"description\" fields of travel-sample-index-stored): ");

        for (SearchQueryRow row: mySearchQueryResult07)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 8. Phrase query.
        //
        // Currently returns nothing.
        //
        PhraseQuery myPhraseQuery08 = SearchQuery.phrase("the few rooms with")
                .field("description");

        SearchQueryResult mySearchQueryResult08 = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myPhraseQuery08).limit(100).highlight());

        System.out.println("Query 8: (Phrase query on \"the few rooms with\" in \"description\" fields of travel-sample-index-stored): ");

        for (SearchQueryRow row: mySearchQueryResult08)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 9. Match with a custom analyzer. The single analyzer ensures that input bytes are a single
        // token, and won't be broken up at punctuation or special-character boundaries.
        //
        MatchQuery myMatchQuery9 = SearchQuery.match("info@hotelnikkosf.com").field("email")

                // This analyzer must be of the "single" type, and must be already defined on
                // Couchbase Server as "singleAnalyzer").
                //
                // FIX: Works without the analyzer, but not with!
                //
                .analyzer("singleAnalyzer");

        SearchQueryResult mySearchQueryResult9 = travelSample.query(
                new SearchQuery( "travel-sample-index-unstored", myMatchQuery9)
                        .limit(10)
        );

        System.out.println("Query 9 (MatchQuery on \"between O'Farrell and Ellis\" in \"directions\" fields of travel-sample-index-unstored, using single analyzer, defined on Couchbase Server): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult9)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 10. Conjunction-set of different match queries.
        //
        MatchQuery myMatchQueryForConjunctionNumber1 = SearchQuery.match("La Rue Saint Denis!!").field("reviews.content");

        MatchQuery myMatchQueryForConjunctionNumber2 = SearchQuery.match("boutique").field("description");

        // Create a conjunction query that takes the common subset of each of the two defined match queries.
        //
        ConjunctionQuery myConjunctionQuery10 = SearchQuery.conjuncts(myMatchQueryForConjunctionNumber1, myMatchQueryForConjunctionNumber2);

        SearchQueryResult mySearchQueryResult10  = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myConjunctionQuery10)
                        .limit(10).highlight()
        );

        System.out.println("Query 10 (Conjunction of two match queries - \"La Rue Saint Denis!!\" and \"boutique\", respectively on the \"reviews.content\" and \"description\" fields - of travel-sample-index-stored): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult10)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 11: Query String Query. Note the specification of the target-field within the
        // query string.
        //
        QueryStringQuery myQueryStringQuery11 = SearchQuery.queryString("description: Imperial");

        SearchQueryResult mySearchQueryResult11  = travelSample.query(
                new SearchQuery( "travel-sample-index-unstored", myQueryStringQuery11)
                        .limit(10)
        );

        System.out.println("Query 11 (Query String Query on travel-sample-index-unstored): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult11)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 12: Wildcard Query. Note the specification of the word "boutique", using
        // a wildcard-character.
        //
        WildcardQuery myWildcardQuery12 = SearchQuery.wildcard("bouti*ue")
                .field("description");

        SearchQueryResult mySearchQueryResult12  = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myWildcardQuery12)
                        .limit(10).highlight()

        );

        System.out.println("Query 12 (WildcardQuery on travel-sample-index-stored): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult12)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 13: Numeric Range Query. Returns all documents whose id is between the stated minimum
        // and maximum values.
        //
        NumericRangeQuery myNumericRangeQuery13 = SearchQuery.numericRange().min(10100).max(10200).field("id");

        SearchQueryResult mySearchQueryResult13  = travelSample.query(
                new SearchQuery( "travel-sample-index-unstored", myNumericRangeQuery13)
                        .limit(10)
        );

        System.out.println("Query 13 (NumericRangeQuery on travel-sample-index-unstored): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult13)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // Query 14: Regexp Query.
        //
        //
        RegexpQuery myRegexpQuery14 = SearchQuery.regexp("[a-z]").field("description");

        SearchQueryResult mySearchQueryResult14  = travelSample.query(
                new SearchQuery( "travel-sample-index-stored", myRegexpQuery14)
                        .limit(10).highlight()
        );

        System.out.println("Query 14 (RegexpQuery on travel-sample-index-stored): ");
        System.out.println('\n');

        for (SearchQueryRow row : mySearchQueryResult14)
        {
            System.out.println(row);
        }

        System.out.println('\n');
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println("= = = = = = = = = = = = = = = = = = = = = = =");
        System.out.println('\n');

        // As administrator, disconnect from cluster.
        //
        System.out.println('\n');
        System.out.println("Administrator disconnecting.");
        cluster.disconnect();
    }
}