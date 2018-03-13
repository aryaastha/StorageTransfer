package Utils;

import Bean.FilterDetails;

/**
 * Created by astha.a on 11/05/17.
 */
public class QueryBuilder {
    public static StringBuilder builder(FilterDetails[] filters, String[] whiteList, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT").append(" ");
        if (whiteList.length == 0) {
            stringBuilder.append("* ");
        } else {
            stringBuilder.append(whiteList[0]).append(" ");
            for (int i = 1; i < whiteList.length; i++) {
                stringBuilder.append(", ").append(whiteList[i]).append(" ");
            }
        }

        stringBuilder.append("FROM ").append(tableName).append(" ");

        if (filters.length > 0) {
            stringBuilder.append("WHERE ");
            stringBuilder.append(filters[0].field).append(" ");
            stringBuilder.append("BETWEEN ");
            stringBuilder.append("\"").append(filters[0].start).append("\" and ");
            stringBuilder.append("\"").append(filters[0].end).append("\" ");
            for (int i = 1; i < filters.length; i++) {
                stringBuilder.append("and ");
                stringBuilder.append(filters[i].field).append(" ");
                stringBuilder.append("BETWEEN ");
                stringBuilder.append("\"").append(filters[i].start).append("\" and ");
                stringBuilder.append("\"").append(filters[i].end).append("\" ");
            }
        }

        System.out.println("Query built for hive is : " + stringBuilder);

        return stringBuilder;
    }
}
