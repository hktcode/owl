/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableList;
import com.hktcode.lang.exception.ArgumentNullException;

import java.time.Duration;
import java.util.List;

public class CompareReport
{
    public static CompareReport of //
        /* */( Duration duration //
        /* */, Duration oldtuple //
        /* */, Duration newtuple //
        /* */, ImmutableList<Duration> insertts //
        /* */, ImmutableList<Duration> updatets //
        /* */, ImmutableList<Duration> deletets //
        /* */) //
    {
        if (duration == null) {
            throw new ArgumentNullException("duration");
        }
        if (oldtuple == null) {
            throw new ArgumentNullException("oldtuple");
        }
        if (newtuple == null) {
            throw new ArgumentNullException("newtuple");
        }
        if (insertts == null) {
            throw new ArgumentNullException("insertts");
        }
        if (updatets == null) {
            throw new ArgumentNullException("updatets");
        }
        if (deletets == null) {
            throw new ArgumentNullException("deletets");
        }

        return new CompareReport(duration, oldtuple, newtuple, insertts, updatets, deletets);
    }
    public final Duration oldtuple;

    public final Duration newtuple;

    public final ImmutableList<Duration> insertts;

    public final ImmutableList<Duration> updatets;

    public final ImmutableList<Duration> deletets;

    public final Duration duration;

    private CompareReport //
        /* */( Duration duration //
        /* */, Duration oldtuple //
        /* */, Duration newtuple //
        /* */, ImmutableList<Duration> insertts //
        /* */, ImmutableList<Duration> updatets //
        /* */, ImmutableList<Duration> deletets //
        /* */) //
    {
        this.duration = duration;
        this.oldtuple = oldtuple;
        this.newtuple = newtuple;
        this.insertts = insertts;
        this.updatets = updatets;
        this.deletets = deletets;
    }

    private static void append(Duration duration, StringBuilder builder)
    {
        builder.append('"');
        builder.append(duration.toString());
        builder.append('"');
    }

    private static void append(List<Duration> durations, StringBuilder builder)
    {
        builder.append('[');
        int size = durations.size();
        if (size > 0) {
            append(durations.get(0), builder);
            for (int i = 1; i < size; ++i) {
                builder.append(',');
                builder.append(' ');
                append(durations.get(i), builder);
            }
        }
        builder.append(']');
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("\tduration: ");
        append(duration, sb);
        sb.append("\n\toldtuple: ");
        append(oldtuple, sb);
        sb.append("\n\tnewtuple: ");
        append(newtuple, sb);
        sb.append("\n\tinsertts: ");
        append(insertts, sb);
        sb.append("\n\tupdatets: ");
        append(updatets, sb);
        sb.append("\n\tdeletets: ");
        append(deletets, sb);
        return sb.toString();
    }

}
