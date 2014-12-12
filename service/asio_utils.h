/* asio_utils.h                                                      -*-C++-*-
   Wolfgang Sourdeau, December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#include <boost/asio/buffer.hpp>


namespace Datacratic {

using boost::asio::const_buffer;

struct const_buffers_2
{
    typedef const_buffer value_type;
    typedef const const_buffer * const_iterator;

    /// Construct to represent a given memory range.
    const_buffers_2(const const_buffer & buffer1,
                    const const_buffer & buffer2)
    {
        buffers[0] = buffer1;
        buffers[1] = buffer2;
    }

    /// Get a random-access iterator to the first element.
    const_iterator begin() const
    {
        return buffers;
    }

    /// Get a random-access iterator for one past the last element.
    const_iterator end() const
    {
        return buffers + 2;
    }

private:
    const_buffer buffers[2];
};

} // namespace Datacratic
