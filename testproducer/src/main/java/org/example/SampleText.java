package org.example;

import java.util.ArrayList;
import java.util.List;

public class SampleText {
    public static final String text = "word count from Wikipedia the free encyclopedia\n" +
            "the word count is the number of words in a document or passage of text Word counting may be needed when a text\n" +
            "is required to stay within certain numbers of words This may particularly be the case in academia legal\n" +
            "proceedings journalism and advertising Word count is commonly used by translators to determine the price for\n" +
            "the translation job Word counts may also be used to calculate measures of readability and to measure typing\n" +
            "and reading speeds usually in words per minute When converting character counts to words a measure of five or\n" +
            "six characters to a word is generally used Contents Details and variations of definition Software In fiction\n" +
            "In non fiction See also References Sources External links Details and variations of definition\n" +
            "This section does not cite any references or sources Please help improve this section by adding citations to\n" +
            "reliable sources Unsourced material may be challenged and removed\n" +
            "Variations in the operational definitions of how to count the words can occur namely what counts as a word and\n" +
            "which words don't count toward the total However especially since the advent of widespread word processing there\n" +
            "is a broad consensus on these operational definitions and hence the bottom line integer result\n" +
            "The consensus is to accept the text segmentation rules generally found in most word processing software including how\n" +
            "word boundaries are determined which depends on how word dividers are defined The first trait of that definition is that a space any of various whitespace\n" +
            "characters such as a regular word space an em space or a tab character is a word divider Usually a hyphen or a slash is too\n" +
            "Different word counting programs may give varying results depending on the text segmentation rule\n" +
            "details and on whether words outside the main text such as footnotes endnotes or hidden text) are counted But the behavior\n" +
            "of most major word processing applications is broadly similar However during the era when school assignments were done in\n" +
            "handwriting or with typewriters the rules for these definitions often differed from todays consensus\n" +
            "Most importantly many students were drilled on the rule that certain words don't count usually articles namely a an the but\n" +
            "sometimes also others such as conjunctions for example and or but and some prepositions usually to of Hyphenated permanent\n" +
            "compounds such as follow up noun or long term adjective were counted as one word To save the time and effort of counting\n" +
            "word by word often a rule of thumb for the average number of words per line was used such as 10 words per line These rules\n" +
            "have fallen by the wayside in the word processing era the word count feature of such software which follows the text\n" +
            "segmentation rules mentioned earlier is now the standard arbiter because it is largely consistent across documents and\n" +
            "applications and because it is fast effortless and costless already included with the application As for which sections of\n" +
            "a document count toward the total such as footnotes endnotes abstracts reference lists and bibliographies tables figure\n" +
            "captions hidden text the person in charge teacher client can define their choice and users students workers can simply\n" +
            "select or exclude the elements accordingly and watch the word count automatically update Software Modern web browsers\n" +
            "support word counting via extensions via a JavaScript bookmarklet or a script that is hosted in a website Most word\n" +
            "processors can also count words Unix like systems include a program wc specifically for word counting\n" +
            "As explained earlier different word counting programs may give varying results depending on the text segmentation rule\n" +
            "details The exact number of words often is not a strict requirement thus the variation is acceptable\n" +
            "In fiction Novelist Jane Smiley suggests that length is an important quality of the novel However novels can vary\n" +
            "tremendously in length Smiley lists novels as typically being between and words while National Novel Writing Month\n" +
            "requires its novels to be at least words There are no firm rules for example the boundary between a novella and a novel\n" +
            "is arbitrary and a literary work may be difficult to categorise But while the length of a novel is to a large extent up\n" +
            "to its writer lengths may also vary by subgenre many chapter books for children start at a length of about words and a\n" +
            "typical mystery novel might be in the to word range while a thriller could be over words\n" +
            "The Science Fiction and Fantasy Writers of America specifies word lengths for each category of its Nebula award categories\n" +
            "Classification\tWord count Novel over words Novella to words Novelette to words Short story under words\n" +
            "In non fiction The acceptable length of an academic dissertation varies greatly dependent predominantly on the subject\n" +
            "Numerous American universities limit Ph.D. dissertations to at most words barring special permission for exceeding this limit";
    public static List<String> toFiveWordStrings() {
        String[] words = text.split("\\s+");
        List<String> result = new ArrayList<>();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            builder.append(words[i]).append(" ");

            if ((i + 1) % 5 == 0) {
                result.add(builder.toString().trim());
                builder = new StringBuilder();
            }
        }

        if (!builder.toString().trim().isEmpty()) {
            result.add(builder.toString().trim());
        }

        return result;
    }
}
