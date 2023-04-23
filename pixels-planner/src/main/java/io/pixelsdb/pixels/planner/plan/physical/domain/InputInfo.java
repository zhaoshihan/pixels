/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.Objects;

/**
 * @author hank
 * @create 2022-06-02
 */
public class InputInfo {
    private String path;
    private int rgStart;
    /**
     * The number of row groups to be scanned from each file.
     */
    private int rgLength;

    /**
     * Default constructor for Jackson.
     */
    public InputInfo() { }

    /**
     * Create the input information for a Pixels file.
     *
     * @param path the path of the file
     * @param rgStart the row group id to start reading
     * @param rgLength the number of row groups to read, if it is non-positive,
     *                 it means read to the last row group in the file
     */
    public InputInfo(String path, int rgStart, int rgLength) {
        this.path = path;
        this.rgStart = rgStart;
        this.rgLength = rgLength;
    }

    private InputInfo(Builder builder) {
        this(builder.path, builder.rgStart, builder.rgLength);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getRgStart()
    {
        return rgStart;
    }

    public void setRgStart(int rgStart)
    {
        this.rgStart = rgStart;
    }

    public int getRgLength()
    {
        return rgLength;
    }

    public void setRgLength(int rgLength)
    {
        this.rgLength = rgLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputInfo inputInfo = (InputInfo) o;
        return rgStart == inputInfo.rgStart && rgLength == inputInfo.rgLength && path.equals(inputInfo.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, rgStart, rgLength);
    }

    public static final class Builder {
        private String path;
        private int rgStart;
        private int rgLength;

        private Builder() {}

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setRgStart(int rgStart) {
            this.rgStart = rgStart;
            return this;
        }

        public Builder setRgLength(int rgLength) {
            this.rgLength = rgLength;
            return this;
        }

        public InputInfo build() {
            return new InputInfo(this);
        }
    }
}
