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

import java.util.List;
import java.util.Objects;

/**
 * @author hank
 * @create 2022-06-02
 */
public class InputSplit
{
    private List<InputInfo> inputInfos;

    /**
     * Default constructor for Jackson.
     */
    public InputSplit() {}

    public InputSplit(List<InputInfo> inputInfos)
    {
        this.inputInfos = inputInfos;
    }

    public List<InputInfo> getInputInfos() {
        return inputInfos;
    }

    public void setInputInfos(List<InputInfo> inputInfos) {
        this.inputInfos = inputInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputSplit that = (InputSplit) o;
        return inputInfos.equals(that.inputInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputInfos);
    }
}
