package dev.donhk.stream;

import dev.donhk.pojos.ElasticRow;
import dev.donhk.pojos.ElasticRowCol;
import dev.donhk.pojos.UserTxn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class UserTxn2ElasticRow  extends PTransform<PCollection<KV<Long, UserTxn>>, PCollection<KV<Long, ElasticRow>>> {
    public static UserTxn2ElasticRow of() {
        return new UserTxn2ElasticRow();
    }

    @Override
    public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, UserTxn>> input) {
        return input.apply(MapElements.into(TypeDescriptors.kvs(
                        TypeDescriptors.longs(),
                        TypeDescriptor.of(ElasticRow.class)))
                .via((SerializableFunction<KV<Long, UserTxn>, KV<Long, ElasticRow>>) input1 -> {
                            final ElasticRow row = ElasticRow.of();
                            final UserTxn txn = input1.getValue();
                            row.addCol(ElasticRowCol.ID, txn.getId());
                            row.addCol(ElasticRowCol.EMAIL, txn.getEmail());
                            row.addCol(ElasticRowCol.FIRST_NAME, txn.getFirstName());
                            row.addCol(ElasticRowCol.SECOND_NAME, txn.getSecondName());
                            row.addCol(ElasticRowCol.GENDER, txn.getGender());
                            row.addCol(ElasticRowCol.TIME, txn.getTime());
                            row.addCol(ElasticRowCol.AMOUNT, txn.getAmount());
                            row.addCol(ElasticRowCol.MATCH, txn.getMatch());
                            row.addCol(ElasticRowCol.MEMORY, txn.getMemory());
                            return KV.of(input1.getKey(), row);
                        }
                ));
    }
}
